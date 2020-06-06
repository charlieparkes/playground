-include $(shell curl -sSL -o .build-harness "https://raw.githubusercontent.com/mintel/build-harness/master/templates/Makefile.build-harness"; echo .build-harness)

.PHONY: init
init: bh/init
	@$(MAKE) bh/venv pipenv

# env: pipenv
# 	@#if [ ! -f "activate" ]; then ln -s .venv/bin/activate activate; fi
# .PHONY: env

.PHONY: shell
shell: pipenv ; $(WITH_PIPENV) python

.PHONY: clean
clean: pipenv/clean python/clean bh/clean

.PHONY: diagrams
diagrams: pipenv ; $(WITH_PIPENV) python diagrams/enrichment.py

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
