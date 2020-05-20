poetryversion:
	poetry version $(version) 
	
version: poetryversion
	$(eval NEW_VERS := $(shell cat pyproject.toml | grep "^version = \"*\"" | cut -d'"' -f2))
	sed -i "" "s/__version__ = .*/__version__ = \"$(NEW_VERS)\"/g" gordo_dataset/__init__.py
