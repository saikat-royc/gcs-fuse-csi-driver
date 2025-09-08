#!/bin/bash

# Title: setup_gke_workload_identity.sh
# Description: Creates KSA (if needed), GSA (if needed), configures GKE
#              Workload Identity for the KSA, and grants a specified
#              role to the GSA on a specific GCS bucket using 'gsutil'.
#              The script is designed to be idempotent.

# Exit on error, treat unset variables as error, propagate pipeline errors
set -eu -o pipefail

# --- Input Argument Handling ---
if [ -z "${1:-}" ]; then
  # Print usage message to standard error
  echo "Usage: $0 <target-bucket-name>" >&2
  echo "  Error: Target GCS bucket name argument is required." >&2
  exit 1
fi
# Assign the first command-line argument to BUCKET_NAME
BUCKET_NAME="$1"

# --- Configuration ---
# !! Review and update these values if necessary !!
PROJECT_ID="saikatroyc-stateful-joonix"        # Your Google Cloud Project ID
GSA_NAME="gcs-scanner-gsa"                     # Desired name for the Google Service Account (GSA)
KSA_NAME="scanner-ksa"                         # Name of the Kubernetes Service Account (KSA)
K8S_NAMESPACE="default"                        # Namespace of the Kubernetes Service Account
GCP_ROLE="roles/storage.legacyBucketReader"    # IAM Role to grant (includes storage.buckets.get)

# --- Derived Variables ---
GSA_EMAIL="${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
WI_POOL_MEMBER="serviceAccount:${PROJECT_ID}.svc.id.goog[${K8S_NAMESPACE}/${KSA_NAME}]"

# --- Script Logic ---

echo "Starting Workload Identity setup for KSA [${K8S_NAMESPACE}/${KSA_NAME}]..."
echo "  Project ID:        ${PROJECT_ID}"
echo "  KSA Name:          ${KSA_NAME}"
echo "  KSA Namespace:     ${K8S_NAMESPACE}"
echo "  GSA Name:          ${GSA_NAME}"
echo "  GSA Email:         ${GSA_EMAIL}"
echo "  Target Bucket:     ${BUCKET_NAME}"
echo "  IAM Role to grant: ${GCP_ROLE}"
echo "  Workload ID Member:${WI_POOL_MEMBER}"
echo ""

# 1. Create Kubernetes Service Account (KSA) - Idempotent Check
echo "[Step 1/5] Checking/Creating Kubernetes Service Account (${KSA_NAME}) in namespace [${K8S_NAMESPACE}]..."
# Check if KSA exists by trying to get it, suppress command output.
# The exit code will be non-zero if it doesn't exist.
if ! kubectl get serviceaccount "${KSA_NAME}" --namespace "${K8S_NAMESPACE}" -o name > /dev/null 2>&1; then
    echo "  KSA [${KSA_NAME}] does not exist in namespace [${K8S_NAMESPACE}]. Creating..."
    kubectl create serviceaccount "${KSA_NAME}" --namespace "${K8S_NAMESPACE}"
    echo "  KSA [${KSA_NAME}] created."
else
    echo "  KSA [${KSA_NAME}] already exists in namespace [${K8S_NAMESPACE}]. Skipping creation."
fi
echo "--------------------"


# 2. Create Google Service Account (GSA) - Idempotent Check
echo "[Step 2/5] Checking/Creating Google Service Account (${GSA_EMAIL})..."
# Check if GSA exists by trying to describe it. Suppress stdout/stderr on success.
if ! gcloud iam service-accounts describe "${GSA_EMAIL}" --project="${PROJECT_ID}" --quiet > /dev/null 2>&1; then
    echo "  GSA [${GSA_EMAIL}] does not exist. Creating..."
    gcloud iam service-accounts create "${GSA_NAME}" \
      --project="${PROJECT_ID}" \
      --display-name="GSA for ${KSA_NAME} KSA (${BUCKET_NAME} access)"
    echo "  GSA [${GSA_EMAIL}] created."
else
    echo "  GSA [${GSA_EMAIL}] already exists. Skipping creation."
fi
echo "--------------------"

# 3. Grant necessary IAM Role to GSA on the specific bucket - USING GSUTIL
#    'gsutil iam ch' additively modifies the policy and is idempotent for grants.
#    Syntax: gsutil iam ch <member>:<role> gs://<bucket>
#    Member format for GSA: serviceAccount:<email>
echo "[Step 3/5] Ensuring IAM role [${GCP_ROLE}] for GSA [${GSA_EMAIL}] on bucket [${BUCKET_NAME}] using gsutil..."
gsutil iam ch "serviceAccount:${GSA_EMAIL}:${GCP_ROLE}" "gs://${BUCKET_NAME}"
echo "  Role binding on bucket ensured via gsutil."
echo "--------------------"

# 4. Allow KSA to impersonate GSA (Workload Identity Binding)
#    'add-iam-policy-binding' is additive and generally idempotent for a specific member/role.
echo "[Step 4/5] Ensuring Workload Identity binding between KSA [${K8S_NAMESPACE}/${KSA_NAME}] and GSA [${GSA_EMAIL}]..."
gcloud iam service-accounts add-iam-policy-binding "${GSA_EMAIL}" \
  --role="roles/iam.workloadIdentityUser" \
  --member="${WI_POOL_MEMBER}" \
  --project="${PROJECT_ID}"
echo "  Workload Identity binding ensured."
echo "--------------------"

# 5. Annotate Kubernetes Service Account (KSA)
#    '--overwrite' makes this command idempotent.
echo "[Step 5/5] Annotating KSA [${K8S_NAMESPACE}/${KSA_NAME}]..."
kubectl annotate serviceaccount "${KSA_NAME}" \
  --namespace "${K8S_NAMESPACE}" \
  iam.gke.io/gcp-service-account="${GSA_EMAIL}" \
  --overwrite
echo "  KSA annotation ensured."
echo "--------------------"


echo "✅ Workload Identity setup script completed successfully for KSA [${K8S_NAMESPACE}/${KSA_NAME}] targeting bucket [${BUCKET_NAME}]!"
echo "Ensure the Pod uses serviceAccountName: ${KSA_NAME}"
echo "If the Pod was already running, it might need restarting to pick up the Workload Identity changes."