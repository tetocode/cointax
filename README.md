利用している取引所の売買履歴・入出金履歴をAPI(APIある場合)で取得し、共通のフォーマットに変換して所得計算を行うためのツール群。

DBはMongoDB。

APIがなくcsvのみ提供しているものはcsv変換プログラム。

みんなのビットコインはPDFのみのため、pdftotextでテキスト抽出、パースして変換するようにした。