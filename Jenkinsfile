pipeline {
  agent any
  parameters {
    choice(name: 'ENVIRONMENT', choices: ['dev', 'staging', 'production'], description: 'Target environment')
    booleanParam(name: 'KAFKA_CLEANUP_TOPICS', defaultValue: false, description: 'Clean Kafka topics before deployment')
    booleanParam(name: 'KAFKA_DELETE_UNWANTED_TOPICS', defaultValue: false, description: 'Delete non-whitelisted topics')
    booleanParam(name: 'ALLOW_PROD_KAFKA_CLEANUP', defaultValue: false, description: 'Allow destructive Kafka cleanup in production')
  }
  stages {
    stage('Render') { steps { sh 'kubectl kustomize k8s/overlays/${ENVIRONMENT}' } }
    stage('Kafka Topics') { steps { echo 'Run scripts/kafka/apply-topics.sh from a Jenkins agent with Kafka CLI access.' } }
    stage('Deploy') { steps { echo 'Apply Kubernetes manifests and wait for rollouts.' } }
    stage('Smoke') { steps { echo 'Run smoke tests and integration gate.' } }
  }
}
