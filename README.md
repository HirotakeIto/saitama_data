# Saitama Data
埼玉県学力調査のデータをセットアップするプロジェクトフォルダです。
現状、伊藤寛武によってメンテナンスされています。

##  System Dependencies
以下の構成での動作を確認しています。

* Python: >= `3.6`
* PostgreSQL: >= `9.5`
* pip
* pyenv
* poetry

## フォルダ構成
```
.
├── data: データを格納しているディレクトリ
├── document: 各種ドキュメント
├── saitama_data: 諸コードを格納しているディレクトリ
│   ├── setting.ini: プロジェクトのconfigを記述するファイル
│   └── setting_sample.ini: `setting.ini`のサンプル
├── tests: テストディレクトリ。snapshottestにてクリーニングデータのテストを行っている。
├── .python-version: 利用するpython環境(pyenv)。
├── README.md
├── poetry.lock: poetryが生成する本レポジトリで利用するpythonパッケージとその依存関係を記述したファイル。
└── pyproject.toml: poetryの設定ファイル。
```

## データについて
県からうけとる埼玉県学力調査データは（色々な事情があり）、そのままでは用いることが出来ません。
そのため、これまで受け取ったデータを加工＆クリーニングしてデータ生成をするということを行ってきた。

### 元データ
埼玉県学力調査は県から受け取った後、適切に加工してRDBに格納しております。
`./data/original_data/`には、その大元になる受け取ったデータを全て配置されています。
具体的には、埼玉県からは次の様なファイルを受け取っています。

* document/output/tree_output

    受け取ったファイルを全て示したファイル。次のコマンドによって生成

    `tree  -d ./data/original_data/ -o document/output/tree_output`

* document/output/tree_output_including_files

    受け取ったファイルのフォルダ構造だけ抜き出したファイル。次のコマンドによって生成

    `tree  ./data/original_data/ -o document/output/tree_output_including_files`


しかし、当然、個人情報保護などの観点からこれらのファイルはgit上では管理されていない。
もし大元のファイルにアクセスをしたい人は伊藤寛武に連絡をください。

### カラム情報ファイル
TDB

### 他

* `./data/dump/`: 各種データをdumpしたファイルを配置している。加工済みcsvファイルはこのフォルダに格納されている。
* `./data/db`: RDBに格納していないデータを保存している。
* `./data/info/`: データベース情報を配置している。

## 利用方法
環境をセットアップした後に次のように本レポジトリのコードを利用することができる。

* 元データからデータのセットアップ
``` python
$ python saitama_data/datasetup/models/seed.py
```

* データの取得
本レポジトリを利用することで、データを取得することができます。
今現在準備しているのは、次のデータです。

`IdMaster, Gakuryoku, SeitoQes, SchoolQes, SeitoQesSosei`

これら次の様に利用することができます。
``` ipython
In[2]: from saitama_data.datasetup.models import IdMaster
In[4]: idmaster = IdMaster()
In[5]: idmaster.read()
In[10]: idmaster.data.head(1)
Out[10]:
   city_id  class  grade   ...    school_id  sex    year
0  26646.0    1.0    6.0   ...      10276.0  1.0  2015.0

[1 rows x 8 columns]
```
こんな感じでデータを `panads` の `DataFrame` の形で取得することができます。
