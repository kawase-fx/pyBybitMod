# pyBybitMod

### ざっくりいうと

https://gist.github.com/MtkN1/50923f4366c3390a1cf4310f77ea7440 をフォークして修正した版

### クラスファイル

https://raw.githubusercontent.com/kawase-fx/pyBybitMod/main/pybybitMod.py

#### 必要パッケージのインストール

`pip install -U -r requiredPackages`

### サンプル

#### APIキーファイルの準備
ファイルの名前は何でもいいです。
2行のテキストファイルを準備します。
* 1行目はAPIキー
* 2行目はAPIシークレット
* 3行目はテストネットかどうか
* 4行目は商品(BTCUSDとか)
を書き込んでおきます。

test-netの利用は、それ用のサイトでAPIキーを作る必要があります。

#### 起動
$ python example.py APIキーファイル名

### 改変箇所

* 文字列定数化
* たいむすたんぷ関係
* うぇぶそけチャンネル名
* RESTオーダーブックのエントリポイント追加
* VSCodeで編集しやすいよう（俺視点）インデント変更

### その他

使いそうな場所だけテストしたので、ほかでエラーが出たり矛盾しててもしりませんが、言ってくれたら対応するかもです。
