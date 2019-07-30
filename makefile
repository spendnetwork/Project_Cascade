install_core:
	-git clone git@github.com:spendnetwork/core.git ../core

install: install_core
	pipenv install
	pip install -e ../core