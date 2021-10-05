#!/bin/bash
# ecr_update_required.sh {IMAGE_NAME} {ECR_REPOSITORY_NAME}
# This script will succeed if the currently running image has a digest
# different from the digest of the image tagged as latest in ECR.
# Exit Codes:
#   (0) update required
#   (1) error
#   (2) no update required
IMAGE_NAME=$1
ECR_REPOSITORY_NAME=$2
RUNNING_CONTAINER=$(docker ps --filter "name=^/$${IMAGE_NAME}\$" \
                          --filter "status=running" \
                          --format "{{.ID}}")
if [ -z $RUNNING_CONTAINER ]; then echo "ERROR: '$${IMAGE_NAME}' is not running!"; exit 1; fi
RUNNING_IMAGE=$(docker container inspect $${RUNNING_CONTAINER} --format='{{index .Image}}')
RUNNING_IMAGE_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' $${RUNNING_IMAGE} \
                                      | sed 's/.*\(sha256:[0-9a-z]*\).*/\1/g')
DEFAULT_AWS_REGION=$(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone | rev | cut -c 2- | rev)
if [ -z $DEFAULT_AWS_REGION ]; then echo "ERROR: Failed to fetch AWS region!"; exit 1; fi
ECR_IMAGE_DIGEST=$(/opt/bin/aws ecr describe-images --repository-name $${ECR_REPOSITORY_NAME} \
                                                    --image-ids imageTag=latest \
                                                    --output json --region $${DEFAULT_AWS_REGION} \
                                                    | jq -r '.imageDetails[0].imageDigest')
if ! [ -z $${DEBUG+x} ]; then echo -e "Local: $${RUNNING_IMAGE_DIGEST}\nRemote: $${ECR_IMAGE_DIGEST}"; fi
if [ "$RUNNING_IMAGE_DIGEST" == "$ECR_IMAGE_DIGEST" ]; then exit 2; fi
