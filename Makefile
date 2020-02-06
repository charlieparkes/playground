-include $(shell curl -sSL -o .build-harness "https://raw.githubusercontent.com/mintel/build-harness/master/templates/Makefile.build-harness"; echo .build-harness)

init: init-build-harness
	@make pipenv
.PHONY: init

env: pipenv
	@#if [ ! -f "activate" ]; then ln -s .venv/bin/activate activate; fi
.PHONY: env

shell:
	$(WITH_PIPENV) python
.PHONY: shell

#ENV ?= dev
#AWS_AUTH := $(WITH_PIPENV) awsauth --profile=everest-$(ENV)
#FOO := $(AWS_AUTH); aws sts get-caller-identity

# credentials: env
# 	#$(AWS_AUTH)
# .PHONY: credentials
#
# auth: env
# 	#$(WITH_PIPENV) ./auth.sh dev
# 	#$(WITH_PIPENV) aws sts get-caller-identity
# 	#@echo "\n You're good to go!"
# .PHONY: check
