TASK_DEFINITION=`aws --output json ecs --region us-east-2 describe-task-definition --task-def arn:aws:ecs:us-east-2:583109823770:task-definition/prometheus-ecs-sd:10`
# Get a JSON representation of the current task definition
# + Update definition to use new image name
# + Filter the def
if [[ "x$TAGONLY" == "x" ]]; then
  DEF=$( echo "$TASK_DEFINITION" \
        | sed -e "s|\"image\": *\"${imageWithoutTag}:.*\"|\"image\": \"${useImage}\"|g" \
        | sed -e "s|\"image\": *\"${imageWithoutTag}\"|\"image\": \"${useImage}\"|g" \
        | jq '.taskDefinition' )
else
  DEF=$( echo "$TASK_DEFINITION" \
        | sed -e "s|\(\"image\": *\".*:\)\(.*\)\"|\1${useImage}\"|g" \
        | jq '.taskDefinition' )
fi

# Default JQ filter for new task definition
NEW_DEF_JQ_FILTER="family: .family, volumes: .volumes, containerDefinitions: .containerDefinitions, placementConstraints: .placementConstraints"

# Some options in task definition should only be included in new definition if present in
# current definition. If found in current definition, append to JQ filter.
CONDITIONAL_OPTIONS=(networkMode taskRoleArn placementConstraints)
for i in "${CONDITIONAL_OPTIONS[@]}"; do
  re=".*${i}.*"
  if [[ "$DEF" =~ $re ]]; then
    NEW_DEF_JQ_FILTER="${NEW_DEF_JQ_FILTER}, ${i}: .${i}"
  fi
done

# Updated jq filters for AWS Fargate
REQUIRES_COMPATIBILITIES=$(echo "${DEF}" | jq -r '. | select(.requiresCompatibilities != null) | .requiresCompatibilities[]')
if [[ "${REQUIRES_COMPATIBILITIES}" == 'FARGATE' ]]; then
  FARGATE_JQ_FILTER='executionRoleArn: .executionRoleArn, requiresCompatibilities: .requiresCompatibilities, cpu: .cpu, memory: .memory'
  NEW_DEF_JQ_FILTER="${NEW_DEF_JQ_FILTER}, ${FARGATE_JQ_FILTER}"
fi

# Build new DEF with jq filter
NEW_DEF=$(echo "$DEF" | jq "{${NEW_DEF_JQ_FILTER}}")

# If in test mode output $NEW_DEF
if [ "$BASH_SOURCE" != "$0" ]; then
  echo "$NEW_DEF"
fi
