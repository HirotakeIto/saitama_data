* `pipenv install` の際にフォルダの作成権限を怒られる
```
$ pipenv install Pipfile

（略）
    return str(get_workon_home().joinpath(self.virtualenv_name))
  File "/home/ubuntu/.local/lib/python3.5/site-packages/pipenv/utils.py", line 1414, in get_workon_home
    mkdir_p(str(expanded_path))
  File "/home/ubuntu/.local/lib/python3.5/site-packages/pipenv/utils.py", line 869, in mkdir_p
    os.mkdir(newdir)
PermissionError: [Errno 13] Permission denied: '/home/ubuntu/.local/share/virtualenvs'
```

フォルダの権限を怒られているので
```
$ sudo chmod 777 /home/ubuntu/.local/share
```
で解決