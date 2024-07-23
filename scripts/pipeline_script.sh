#!/bin/bash
set -e

job_files=(
  "01-collect-job-listings.yaml"
  "02-clean-duplicate-ids.yaml"
  "03-enrich-job-listings.yaml"
  "04-clean-duplicate-descriptions.yaml"
  "05-extract-gemini.yaml"
)

for job_file in "${job_files[@]}"; do
  job=$(echo $job_file | sed 's/^[0-9]*-//; s/\.yaml$//')
  
  echo "Starting job: $job"
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
      echo "Job $job completed successfully in $duration seconds"
      break
    elif [ "$status" == "False" ]; then
      echo "Job $job failed"
      kubectl logs job/$job_name
      exit 1
    else
      current_time=$(date +%s)
      elapsed=$((current_time - start_time))
      echo "Job $job still running after $elapsed seconds..."
      
      # Check for pod status
      pod_status=$(kubectl get pods --selector=job-name=$job_name -o jsonpath='{.items[*].status.phase}')
      echo "Pod status: $pod_status"
      
      # If the pod is running, tail the logs
      if [ "$pod_status" == "Running" ]; then
        echo "Recent logs:"
        kubectl logs job/$job_name --tail=5
      fi
    fi
    sleep 300  # Check every 5 minutes
  done

  kubectl delete job $job_name
done

echo "Pipeline completed successfully"