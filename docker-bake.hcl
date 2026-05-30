# Depot bake definition for the scry image.
#
#   depot bake --push                              # build + push :latest
#   TAG=v0.4.0 depot bake --push                   # push only :v0.4.0
#   TAG=v0.4.0 PUSH_LATEST=true depot bake --push  # push :v0.4.0 AND :latest
#
# CI (the tag-triggered release workflow) sets both TAG and PUSH_LATEST=true,
# so a `vX.Y.Z` tag publishes both the version tag and :latest. A bare local
# `TAG=… depot bake` only moves that one tag, never :latest, unless you opt in.
#
# Reads the Depot project id from depot.json. Pushing to docker.io/serialexp
# requires registry auth on the machine running the bake (`docker login`, or
# Depot-configured registry credentials).

variable "TAG" {
  default = "latest"
}

# When "true", also tag the build :latest (on top of :${TAG}). Off by default
# so a one-off local `TAG=… depot bake` doesn't clobber the floating :latest.
variable "PUSH_LATEST" {
  default = "false"
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
  tags       = PUSH_LATEST == "true" ? ["${IMAGE}:${TAG}", "${IMAGE}:latest"] : ["${IMAGE}:${TAG}"]
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
