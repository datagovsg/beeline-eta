# beeline-eta
Prediction of arrival timings for Beeline buses

For each operation, start from the root of the project.

To set up API endpoint:
```
cd api
./app.py
```

For prediction algorithm, create a .env file with DATABASE_URI=<database_uri>.
Then run the following:

To run prediction algorithm in the background:
```
cd main
python main.py
```

To run unit tests:
```
cd main
python -m test
```