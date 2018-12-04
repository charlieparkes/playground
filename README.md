```
make env
. .venv/bin/activate
eval "$(awsauth --profile=everest-dev)" && aws sts get-caller-identity
python
```