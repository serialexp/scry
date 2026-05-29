# Depot bake definition for the scry image.
#
#   depot bake --push                 # build + push :latest to Docker Hub
#   TAG=v0.4.0 depot bake --push      # build + push a version tag
#
# Reads the Depot project id from depot.json. Pushing to docker.io/serialexp
# requires registry auth on the machine running the bake (`docker login`, or
# Depot-configured registry credentials).

variable "TAG" {
  default = "latest"
}

variable "IMAGE" {
  default = "docker.io/serialexp/scry"
}

group "default" {
  targets = ["scry"]
}

target "scry" {
  context    = "."
  dockerfile = "Dockerfile"
  tags       = ["${IMAGE}:${TAG}"]
  platforms  = ["linux/amd64", "linux/arm64"]

  # Supply-chain attestations: full provenance (build inputs/steps) + an SBOM.
  # Satisfies Docker Scout's "supply chain attestation(s)" policy. These attach
  # to the pushed OCI manifest index, so they require `--push` (the local docker
  # image store can't hold them) — fine here, release builds always push.
  attest = [
    "type=provenance,mode=max",
    "type=sbom",
  ]
}
