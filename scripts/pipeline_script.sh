#!/bin/bash
set -eo pipefail

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

job_files=(
  "01-collect-job-listings.yaml"
  "02-clean-duplicate-ids.yaml"
  "03-enrich-job-listings.yaml"
  "04-clean-duplicate-descriptions.yaml"
  "05-extract-gemini.yaml"
  "04-clean-duplicate-descriptions.yaml"
)

for job_file in "${job_files[@]}"; do
  job=$(echo $job_file | sed 's/^[0-9]*-//; s/\.yaml$//')
  
  log "Starting job: $job"
  timestamp=$(date +%s)
  job_name="$job-$timestamp"
  
  kubectl apply -f k8s/jobs/$job_file
  kubectl create job --from=job/$job $job_name

  start_time=$(date +%s)
  while true; do
    status=$(kubectl get job $job_name -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}')
    if [ "$status" == "True" ]; then
      end_time=$(date +%s)
      duration=$((end_time - start_time))
      log "Job $job completed successfully in $duration seconds"
      break
    elif [ "$status" == "False" ]; then
      log "Job $job failed"
      kubectl logs job/$job_name
      exit 1
    else
      current_time=$(date +%s)
      elapsed=$((current_time - start_time))
      log "Job $job still running after $elapsed seconds..."
      
      pod_status=$(kubectl get pods --selector=job-name=$job_name -o jsonpath='{.items[*].status.phase}')
      log "Pod status: $pod_status"
      
      if [ "$pod_status" == "Running" ]; then
        log "Recent logs:"
        kubectl logs job/$job_name --tail=5
      fi
    fi
    sleep 300
  done

  kubectl delete job $job_name
done

log "Pipeline completed successfully"