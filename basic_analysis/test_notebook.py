from sklearn.metrics import accuracy_score
import pandas as pd

import os
import subprocess

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
from nbconvert.preprocessors import CellExecutionError

def test_notebook():
    """
    Проверяем, что ноутбук запускается, отрабатывает без ошибок
    """

    with open('./Python_2_HW_2.ipynb') as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=1800, kernel_name='python3', allow_errors=False)

    try:
        # Check that the notebook runs
        ep.preprocess(nb, {'metadata': {'path': ''}})
    except CellExecutionError:
        raise

    print("Notebook successfully executed`")
