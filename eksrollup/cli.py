import sys
import argparse
import time
import shutil
from .config import app_config
from .lib.logger import logger
from .lib.aws import is_asg_scaled, is_asg_healthy, instance_terminated, get_asg_tag, modify_aws_autoscaling, \
    count_all_cluster_instances, save_asg_tags, get_asgs, scale_asg, plan_asgs, terminate_instance_in_asg, delete_asg_tags, plan_asgs_older_nodes
from .lib.k8s import k8s_nodes_count, k8s_nodes_ready, get_k8s_nodes, modify_k8s_autoscaler, get_node_by_instance_id, \
    drain_node, delete_node, cordon_node, taint_node
from .lib.exceptions import RollingUpdateException


def validate_cluster_health(asg_name, new_desired_asg_capacity, cluster_name, predictive, health_check_type="regular",):
    cluster_health_retry = app_config['CLUSTER_HEALTH_RETRY']
    cluster_health_wait = app_config['CLUSTER_HEALTH_WAIT']
    retry_count = 0

    while retry_count < cluster_health_retry:
        retry_count += 1
        if health_check_type == "asg":
            logger.info(f'Waiting for {cluster_health_wait} seconds for ASG to scale before validating cluster health...')
        else:
            logger.info(f'Waiting for {cluster_health_wait} seconds before validating cluster health...')

        time.sleep(cluster_health_wait)

        # check if asg has enough nodes first before checking instance health
        if not is_asg_scaled(asg_name, new_desired_asg_capacity):
            logger.info(f'Validation failed for asg {asg_name}. Not enough instances online.')
            continue

        # wait and check for instances in ASG to become healthy
        if not is_asg_healthy(asg_name):
            logger.info(f'Validation failed for asg {asg_name}. Some instances not yet healthy.')
            continue

        # wait and check for desired amount of k8s nodes to come online within the cluster
        desired_k8s_node_count = count_all_cluster_instances(cluster_name, predictive=predictive)
        if not k8s_nodes_count(desired_k8s_node_count):
            logger.info(f'Validation failed for cluster {cluster_name}. Didn\'t reach expected node count {desired_k8s_node_count}.')
            continue

        # Wait and check for nodes to become ready
        if not k8s_nodes_ready():
            logger.info('Validation failed for cluster. Expected node count reached but nodes are not ready.')
            continue

        logger.info('Cluster validation passed. Proceeding with node draining and termination...')
        return

    logger.info(f'Exiting since ASG healthcheck failed after {cluster_health_retry} attempts')
    raise Exception('ASG healthcheck failed')


def scale_up_asg(cluster_name, asg, count):
    asg_old_max_size = asg['MaxSize']
    asg_old_desired_capacity = asg['DesiredCapacity']
    desired_capacity = asg_old_desired_capacity + count
    asg_tags = asg['Tags']
    asg_name = asg['AutoScalingGroupName']
    current_capacity = None

    # remove any stale suspensions from asg that may be present
    modify_aws_autoscaling(asg_name, "resume")

    use_asg_termination_policy = app_config['ASG_USE_TERMINATION_POLICY']
    batch_size = app_config['BATCH_SIZE']

    asg_tag_desired_capacity = get_asg_tag(asg_tags, app_config["ASG_DESIRED_STATE_TAG"])
    asg_tag_orig_capacity = get_asg_tag(asg_tags, app_config["ASG_ORIG_CAPACITY_TAG"])
    asg_tag_orig_max_capacity = get_asg_tag(asg_tags, app_config["ASG_ORIG_MAX_CAPACITY_TAG"])

    if desired_capacity == asg_old_desired_capacity:
        logger.info(f'Desired and current capacity for {asg_name} are equal. Skipping ASG.')

        if asg_tag_desired_capacity.get('Value') and asg_tag_orig_capacity.get('Value') and asg_tag_orig_max_capacity.get('Value'):
            logger.info(f'Found capacity tags on ASG {asg_name} from previous run. Leaving alone.')
            return int(asg_tag_desired_capacity.get('Value')), int(asg_tag_orig_capacity.get(
                'Value')), int(asg_tag_orig_max_capacity.get('Value'))
        else:
            save_asg_tags(asg_name, app_config["ASG_ORIG_CAPACITY_TAG"], asg_old_desired_capacity)
            save_asg_tags(asg_name, app_config["ASG_DESIRED_STATE_TAG"], asg_old_desired_capacity)
            save_asg_tags(asg_name, app_config["ASG_ORIG_MAX_CAPACITY_TAG"], asg_old_max_size)
            return asg_old_desired_capacity, asg_old_desired_capacity, asg_old_max_size

    # True: use ASG's 'DesiredCapacity' to count the instances
    # False: use Instances list to count the instances
    predictive = True if use_asg_termination_policy else False

    # only scale up if no previous desired capacity tag set
    if asg_tag_desired_capacity.get('Value'):
        logger.info('Found previous desired capacity value tag set on asg from a previous run.')
        logger.info(f'Maintaining previous capacity of {asg_old_desired_capacity} to not overscale.')

        # check cluster health before doing anything
        validate_cluster_health(
            asg_name,
            int(asg_tag_desired_capacity.get('Value')),
            cluster_name,
            predictive
        )

        return int(asg_tag_desired_capacity.get('Value')), int(asg_tag_orig_capacity.get(
            'Value')), int(asg_tag_orig_max_capacity.get('Value'))
    else:
        logger.info('No previous capacity value tags set on ASG; setting tags.')
        save_asg_tags(asg_name, app_config["ASG_ORIG_CAPACITY_TAG"], asg_old_desired_capacity)
        save_asg_tags(asg_name, app_config["ASG_DESIRED_STATE_TAG"], desired_capacity)
        save_asg_tags(asg_name, app_config["ASG_ORIG_MAX_CAPACITY_TAG"], asg_old_max_size)

        old_desired_capacity = asg_old_desired_capacity

        while True:
            if batch_size:
                if current_capacity is None:
                    current_capacity = old_desired_capacity
                else:
                    old_desired_capacity = current_capacity
                current_capacity += batch_size
                if current_capacity >= desired_capacity:
                    current_capacity = desired_capacity
            else:
                current_capacity = desired_capacity

            # only change the max size if the new capacity is bigger than current max
            if current_capacity > asg_old_max_size:
                scale_asg(asg_name, old_desired_capacity, current_capacity, current_capacity)
            else:
                scale_asg(asg_name, old_desired_capacity, current_capacity, asg_old_max_size)

            # check cluster health before doing anything
            validate_cluster_health(
                asg_name,
                current_capacity,
                cluster_name,
                predictive,
                health_check_type="asg"
            )
            if current_capacity == desired_capacity:
                break
    logger.info('Proceeding with node draining and termination...')
    return desired_capacity, asg_old_desired_capacity, asg_old_max_size


def update_asgs(asgs, cluster_name):
    run_mode = app_config['RUN_MODE']
    use_asg_termination_policy = app_config['ASG_USE_TERMINATION_POLICY']
    batch_size = app_config.get('ROLLOUT_BATCH_SIZE', None)  # e.g. 3 or None

    # pick your planner
    if run_mode == 4:
        asg_outdated = plan_asgs_older_nodes(asgs)
    else:
        asg_outdated = plan_asgs(asgs)

    k8s_nodes, k8s_excluded_nodes = get_k8s_nodes()
    asg_state = {}

    for asg_name, (outdated_instances, asg_obj) in asg_outdated.items():
        total = len(outdated_instances)
        if total == 0:
            continue

        # how many at a time?
        bs = batch_size or total

        # 1) SCALE UP ONCE by batch-size for head-room
        logger.info(f'Scaling ASG {asg_name} up by {bs} for batch head-room')
        new_desired, orig_desired, orig_max = scale_up_asg(cluster_name, asg_obj, bs)
        asg_state[asg_name] = (new_desired, orig_desired, orig_max)

        # save original state tags if your scale_up_asg doesn’t already do it
        save_asg_tags(asg_name, app_config["ASG_DESIRED_STATE_TAG"], new_desired)
        save_asg_tags(asg_name, app_config["ASG_ORIG_CAPACITY_TAG"], orig_desired)
        save_asg_tags(asg_name, app_config["ASG_ORIG_MAX_CAPACITY_TAG"], orig_max)

        # suspend AWS’s own launch/replace if you’re doing manual termination
        if not use_asg_termination_policy:
            modify_aws_autoscaling(asg_name, 'suspend')

        # 2) process in batches
        remaining = list(outdated_instances)
        while remaining:
            batch = remaining[:bs]
            remaining = remaining[bs:]

            # a) cordon/taint for RUN_MODE 2 & 3
            if run_mode in (2, 3):
                for inst in batch:
                    node = get_node_by_instance_id(k8s_nodes, inst['InstanceId'])
                    if app_config.get("TAINT_NODES"):
                        taint_node(node)
                    else:
                        cordon_node(node)

            # b) cordon/taint for RUN_MODE 1 & 4 (with excluded‐node fallback)
            if run_mode in (1, 4):
                for inst in batch:
                    try:
                        node = get_node_by_instance_id(k8s_nodes, inst['InstanceId'])
                    except Exception:
                        # maybe it was already excluded?
                        node = get_node_by_instance_id(k8s_excluded_nodes, inst['InstanceId'])
                        logger.info(f"Node {node} was excluded")
                        continue

                    if app_config.get("TAINT_NODES"):
                        taint_node(node)
                    else:
                        cordon_node(node)

            # c) drain & terminate for RUN_MODE 1,3,4
            if run_mode in (1, 3, 4):
                desired_cap, od, om = asg_state[asg_name]
                for inst in batch:
                    try:
                        node = get_node_by_instance_id(k8s_nodes, inst['InstanceId'])
                    except Exception:
                        node = get_node_by_instance_id(k8s_excluded_nodes, inst['InstanceId'])
                        logger.info(f"Node {node} was excluded")
                        continue

                    drain_node(node)
                    delete_node(node)

                    # decrement and re-save the DESIRED_STATE tag
                    # desired_cap -= 1
                    save_asg_tags(asg_name, app_config["ASG_DESIRED_STATE_TAG"], desired_cap)

                    if not use_asg_termination_policy:
                        terminate_instance_in_asg(inst['InstanceId'])
                        if not instance_terminated(inst['InstanceId']):
                            raise Exception("Instance failed to terminate")
                        wait = app_config.get('BETWEEN_NODES_WAIT', 0)
                        if wait:
                            time.sleep(wait)

                # update our in-memory state so the final scale_asg is correct
                asg_state[asg_name] = (desired_cap, od, om)

            logger.info(f'Processed batch of {len(batch)} nodes for ASG {asg_name}')

        # 3) SCALE BACK ONCE to original desired/max
        new_d, orig_d, orig_m = asg_state[asg_name]
        logger.info(f'Scaling ASG {asg_name} back to original desired {orig_d}')
        scale_asg(asg_name, new_d, orig_d, orig_m)

        # 4) resume AWS policies
        if not use_asg_termination_policy:
            modify_aws_autoscaling(asg_name, 'resume')

        # 5) clean up all tags
        delete_asg_tags(asg_name, app_config["ASG_DESIRED_STATE_TAG"])
        delete_asg_tags(asg_name, app_config["ASG_ORIG_CAPACITY_TAG"])
        delete_asg_tags(asg_name, app_config["ASG_ORIG_MAX_CAPACITY_TAG"])

        logger.info(f'*** Completed rolling update for ASG {asg_name} ***')

    logger.info('All ASGs processed')



def main(args=None):
    parser = argparse.ArgumentParser(description='Rolling update on cluster')
    parser.add_argument('--cluster_name', '-c', required=True,
                        help='the cluster name to perform rolling update on')
    parser.add_argument('--plan', '-p', action='store_const', const=True,
                        help='perform a dry run to see which instances are out of date')
    args = parser.parse_args(args)
    # check kubectl is installed
    kctl = shutil.which('kubectl')
    if not kctl:
        logger.info('kubectl is required to be installed before proceeding')
        quit(1)
    filtered_asgs = get_asgs(args.cluster_name)
    run_mode = app_config['RUN_MODE']
    # perform a dry run on mode 4 for older nodes
    if (args.plan or app_config['DRY_RUN']) and (run_mode == 4):
        plan_asgs_older_nodes(filtered_asgs)
    # perform a dry run on main mode
    elif args.plan or app_config['DRY_RUN']:
        plan_asgs(filtered_asgs)
    else:
        # perform real update
        if app_config['K8S_AUTOSCALER_ENABLED']:
            # pause k8s autoscaler
            modify_k8s_autoscaler("pause")
        try:
            update_asgs(filtered_asgs, args.cluster_name)
            if app_config['K8S_AUTOSCALER_ENABLED']:
                # resume autoscaler after asg updated
                modify_k8s_autoscaler("resume")
            logger.info('*** Rolling update of all asg is complete! ***')
        except Exception as e:
            logger.error(e)
            logger.error('*** Rolling update of ASG has failed. Exiting ***')
            logger.error('AWS Auto Scaling Group processes will need resuming manually')
            if app_config['K8S_AUTOSCALER_ENABLED']:
                logger.error('Kubernetes Cluster Autoscaler will need resuming manually')
            sys.exit(1)
