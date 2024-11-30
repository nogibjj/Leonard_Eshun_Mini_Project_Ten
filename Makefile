install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=my_lib test_*.py

format:	
	black *.py 

lint:
	ruff check *.py my_lib/*.py

container-lint:
	docker run --rm -i hadolint/hadolint < Dockerfile

deploy:
	#deploy goes here

refactor: format lint
		
all: install lint test format
