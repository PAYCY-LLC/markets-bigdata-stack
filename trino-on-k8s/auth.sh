# Generate authorization file
htpasswd -c auth trino
# Create secret in K8S
kubectl create secret generic basic-auth --from-file=auth