@echo off
pushd "%~dp0.."
"estimate\youtube_size_estimator\.venv\Scripts\python.exe" -B "estimate\youtube_size_estimator\estimate.py" --monitor --workers 3 --request-interval 1
popd
