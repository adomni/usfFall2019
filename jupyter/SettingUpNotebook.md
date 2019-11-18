
1. Install python
(Check with python -v)
(Assume that virtualenv came with python, otherwise pip install virtualenv)
2. cd to correct folder or make folder for yourself under jupyter
3. **virtualenv ./venv** (Use venv so it'll be git ignored, otherwise you need to do this yourself)
4. Create requirements file (see example under cweiss/requirements.txt)
5. Switch to your virtual env: **source venv/bin/activate**
6. **pip install -r requirements.txt**
7. **jupyter notebook**
8. Look at opened browser window