.DEFAULT_GOAL := help

ifeq ($(CURRENT_VENV),)
  VIRTUALENV := .venv
else
  VIRTUALENV := $(CURRENT_VENV)
endif

VENV_WORKDIR ?= .

ifeq ($(VENV_WORKDIR),.)
  PIPENV := PIPENV_VENV_IN_PROJECT=1 pipenv
else
  PIPENV := PIPENV_VENV_IN_PROJECT= WORKON_HOME='$(shell realpath $(VENV_WORKDIR))' pipenv
  # if the venv doesn't exist then this will fail, so it will make the VIRTUALENV will be empty
  VIRTUALENV := $(shell $(PIPENV) --venv 2> /dev/null)
  ifneq ($(.SHELLSTATUS),0)
    # means pipenv failed, so it doesn't exist yet. Need to cause a make env
    _ := $(shell $(value PIPENV) install --dev --deploy)
    # this next section doesn't work on the Jenkins box, so don't catch errors
#    ifneq ($(.SHELLSTATUS),0)
#      _ := $(error "Unable to install requirements")
#    endif
    VIRTUALENV := $(shell $(PIPENV) --venv 2> /dev/null)
  endif
endif

WITH_PIPENV := $(PIPENV) run

# This python script generates the help for this Makefile.
define PRINT_HELP_PYSCRIPT
from __future__ import print_function
import re, sys

def print_formatted(target, hlp, indent=20):
	print(("%%-%ss %%s" % (indent,)) % (target, hlp))

def print_makefile_help():
	for line in sys.stdin:
		match = re.match(r'^([a-zA-Z_-]+)\s*:.*?## (.*)$$', line)
		if match:
			target, help = match.groups()
			print_formatted(target, help)

if __name__ == "__main__":
	if len(sys.argv) == 1:
		print_makefile_help()
	else:
		print_formatted(*sys.argv[1:])
endef
export PRINT_HELP_PYSCRIPT

help:
	@echo "Commands:"
	@echo ""
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

$(VIRTUALENV): $(VIRTUALENV)/bin/activate
$(VIRTUALENV)/bin/activate: Pipfile.lock
	$(PIPENV) install --dev --deploy
	if [ "$(VENV_WORKDIR)" = "." ]; then touch $(VIRTUALENV)/bin/activate; fi

env: $(VIRTUALENV) ## create virtualenv
.PHONY: env

.env:
	@read -p "setting x: " SETTING_X; \
	echo "# Port for dev site. Must end in a colon (:)." >> .env; \
	echo "SETTING_X=$$SETTING_X:" >> .env;
	echo "FOOBAR=FOOBAR" >> .env

clean: clean-venv clean-pyc
.PHONY: clean

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +
	find . -name '*.egg-info' -exec rm -fr {} +

clean-venv: ## remove development virtualenv
	rm -rf $(VIRTUALENV)

shell:
	$(WITH_PIPENV) python
.PHONY: shell

quickstart: env
	awsauth --profile=dev
	aws sts get-caller-identity
.PHONY: quickstart
