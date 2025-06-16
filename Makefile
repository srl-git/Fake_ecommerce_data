.PHONY: build-api build-generator

build-api:
	@echo "--- Fetching latest Git short SHA ---"
	@GIT_SHA_SHORT=$(shell git rev-parse --short HEAD) && \
	echo "Using Git SHA for API _VERSION: $$GIT_SHA_SHORT" && \
	echo "--- Submitting API build to Google Cloud Build ---" && \
	gcloud builds submit \
		--config=api/cloudbuild.yaml \
		--substitutions=_VERSION=$$GIT_SHA_SHORT \

build-generator:
	@echo "--- Fetching latest Git short SHA ---"
	@GIT_SHA_SHORT=$(shell git rev-parse --short HEAD) && \
	echo "Using Git SHA for data_generator _VERSION: $$GIT_SHA_SHORT" && \
	echo "--- Submitting data_generator build to Google Cloud Build ---" && \
	gcloud builds submit \
		--config=data_generator/cloudbuild.yaml \
		--substitutions=_VERSION=$$GIT_SHA_SHORT \