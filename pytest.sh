flake8 --exclude=unit_tests,scripts,stoplists,NLTK_data .
coverage run -m pytest
coverage report -m
coverage html
