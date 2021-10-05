def ecs_sqs_autoscaling_metric(
    arrival_rate, service_rate, current_num_tasks, num_messages, scaling_target=100
):
    """
    arrival_rate = sum_one_min("NumberOfMessagesSent")
    service_rate = sum_one_min("NumberOfMessagesDeleted")
    current_num_tasks = get_current_ecs_service_running_task_count()
    num_messages = (
        avg_one_min("ApproximateNumberOfMessagesVisible")
        + avg_one_min("ApproximateNumberOfMessagesNotVisible")
        + avg_one_min("ApproximateNumberOfMessagesDelayed")
    )
    """
    assert arrival_rate >= 0
    assert service_rate >= 0
    assert current_num_tasks >= 0
    assert num_messages >= 0
    assert scaling_target > 0

    if arrival_rate > 0:
        if service_rate > 0:
            # This is the steady state.
            # Messages are arriving and being processed.
            traffic_intensity = arrival_rate / service_rate

            # traffic_intensity has a hard time with scaling down.
            # For example, if there are 10 tasks running, but it only takes 1
            # to handle the current load, then (arrival_rate / service_rate) is
            # still == 1. We want to scale down in that case.
            if 0.95 <= traffic_intensity <= 1.05:
                backlog_intensity = num_messages / current_num_tasks
                if backlog_intensity < 0.95:
                    traffic_intensity = backlog_intensity

            return traffic_intensity * scaling_target

        if current_num_tasks == 0:
            # Scale up from zero.
            return scaling_target * 2

        # Messages are arriving but not getting processed.
        # Keep scaling up until they are. (What else can we do?)
        return (num_messages / current_num_tasks) * scaling_target

    if num_messages == 0:
        # No messages are arriving, and the queue is empty.
        # Scale to zero.
        return 0

    if current_num_tasks == 0:
        # Scale up from zero.
        return scaling_target * 2

    # No new messages are arriving, but the queue isn't empty.
    # Just keep scaling up until the queue is empty or max tasks is reached.
    # Preventing too many tasks is what max tasks is for, after all.
    return (num_messages / current_num_tasks) * scaling_target
