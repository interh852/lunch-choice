import io
import json
import polars as pl
import jpholiday
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Any
import google.auth
from google.cloud import vision
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class MenuList:
    def __init__(self):
        """PDFのメニュー表からCSVのメニュー表を作成"""
        self.service_drive = build(
            serviceName="drive", version="v3", credentials=google.auth.default()[0]
        )
        self.service_sheets = build(
            serviceName="sheets", version="v4", credentials=google.auth.default()[0]
        )
        self.client = storage.Client()
        self.bucket_name = "lunch-choice"
        self.bucket = self.client.bucket(self.bucket_name)
        self.google_drive_info = self.read_gcs_json(
            json_path="credential/google_drive.json"
        )
        self.slack_info = self.read_gcs_json(json_path="credential/slack.json")

    def check_execute(self, operation: str, this_date: date) -> bool:
        """オペレーションを実行するかチェック

        Args:
            operation (str): オペレーション
            this_date (date): 当日の日付

        Returns:
            bool: 実行する場合はTrue, 実行しない場合はFalse
        """
        # Google Driveに保存されているCSVファイルからメニュー表を読み込み
        df_menu = self.read_menu_excel(this_date=this_date)

        df_schedule = (
            df_menu.unique(subset="date")
            .select("date", pl.col(operation))
            .filter(pl.col("date") == this_date)
            .filter(pl.col(operation) == "TRUE")
        )

        if len(df_schedule) > 0:
            return True
        else:
            return False

    # ----------------------------- メニュー表の作成 ----------------------------- #

    def read_gcs_json(self, json_path: str) -> Dict[str, str]:
        """GCSに保存されているGoogle Driveの情報を読み込み

        Args:
            json_path (str): GCSに保存されているJSONファイル

        Returns:
            Dict[str, str]: JSONファイルから読み込んだデータ
        """
        blob = self.bucket.blob(blob_name=json_path)
        google_drive_path = json.load(io.BytesIO(blob.download_as_bytes()))

        return google_drive_path

    def create_menu_spreadsheet(self, this_date: date) -> None:
        """新たにGoogle Driveに追加されたメニュー表をPDFからスプレッドシートに変換しGoogle Driveに保存

        Args:
            this_date (date): 当日の日付
        """
        # Google Driveに新たに追加されたPDFファイルを検索
        pdfs = self.search_drive_files(
            folder_id=self.google_drive_info["FOLDER_PDF"],
            file_type=".pdf",
            search_date=self.get_pastday(this_date=this_date, days=0),
        )

        if pdfs:
            # 最新のPDFファイルを取得
            pdf = pdfs[0]

            # 新たに追加されたPDFファイルをGCSにコピー
            self.copy_menu_from_drive_to_gcs(pdf_info=pdf)

            # PDFから文字情報のjSONファイルを取得
            self.async_detect_document(
                gcs_source_uri=f"gs://{self.bucket_name}/pdf/{pdf['name']}",
                gcs_destination_uri=f"gs://{self.bucket_name}/json/",
            )

            # 文字情報のJSONファイルをメニュー表のスプレッドシートに変換しGoogle Driveに保存
            self.convert_menu_spreadsheet()

    def copy_menu_from_drive_to_gcs(self, pdf_info: Dict[str, str]) -> None:
        """Google DriveからGCSにメニュー表(PDF)をコピー

        Args:
            pdf_info (Dict[str, str]): Google Driveに保存されたメニュー表(PDF)のファイル名とＩＤ
        """
        # Google DriveからPDFファイルをダウンロード
        self.download_drive_file(file_id=pdf_info["id"], filename=pdf_info["name"])

        # GCSにPDFをアップロード
        blob = self.bucket.blob(blob_name=f"pdf/{pdf_info['name']}")
        blob.upload_from_filename(filename=pdf_info["name"])

        print(f"Upload {pdf_info['name']} to GCS")

    def search_drive_files(
        self, folder_id: str, file_type: str, search_date: date
    ) -> List[Dict[str, str]]:
        """Google Driveからファイルを検索

        Args:
            folder_id (str): Google DriveのフォルダID
            file_type (str): ファイルタイプ
            search_date (date): 検索開始する日にち

        Returns:
            List[str]: Google Driveからファイルのリスト
        """
        # 検索条件
        condition_list = [
            f"('{folder_id}' in parents)",
            f"(name contains '{file_type}')",
            f"(createdTime >= '{search_date}')",
        ]
        conditions = " and ".join(condition_list)

        # フォルダ内を検索
        results = (
            self.service_drive.files()
            .list(
                q=conditions,
                fields="nextPageToken, files(id, name)",
                orderBy="createdTime desc",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        files = results.get("files", [])

        return files

    def get_pastday(self, this_date: date, days: int) -> str:
        """過去の日付を取得

        Args:
            this_date (date): 日付
            days (int): 日数

        Returns:
            str: 過去の日付
        """
        return (this_date - timedelta(days=days)).isoformat()

    def download_drive_file(self, file_id: str, filename: str) -> None:
        """Google Driveからファイルのダウンロード

        Args:
            file_id (str): Google DriveにあるファイルのID
            filename (str): Google Driveにあるファイル名
        """
        request = self.service_drive.files().get_media(fileId=file_id)
        file = io.FileIO(filename, "wb")
        downloader = MediaIoBaseDownload(file, request)

        done = False
        while done is False:
            _, done = downloader.next_chunk()

    def async_detect_document(
        self, gcs_source_uri: str, gcs_destination_uri: str
    ) -> None:
        """Cloud Vision APIのOCR機能を使ってPDFから文字情報を取得してJSONファイルとしてGCSに保存

        Args:
            gcs_source_uri (str): PDFのソースが保存されてるGCSのURI
            gcs_destination_uri (str): 文字情報を保存するGCSのURI
        """
        # Supported mime_types are: 'application/pdf' and 'image/tiff'
        mime_type = "application/pdf"

        # How many pages should be grouped into each json output file.
        batch_size = 2

        client = vision.ImageAnnotatorClient()

        feature = vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

        gcs_source = vision.GcsSource(uri=gcs_source_uri)
        input_config = vision.InputConfig(gcs_source=gcs_source, mime_type=mime_type)

        gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
        output_config = vision.OutputConfig(
            gcs_destination=gcs_destination, batch_size=batch_size
        )

        async_request = vision.AsyncAnnotateFileRequest(
            features=[feature], input_config=input_config, output_config=output_config
        )

        operation = client.async_batch_annotate_files(requests=[async_request])

        print("Waiting for the document detection to complete.")
        operation.result(timeout=420)

    def convert_menu_spreadsheet(self) -> None:
        """文字情報のJSONファイルをメニュー表のスプレッドシートに変換しGoogle Driveに保存"""
        # 文字情報のJSONファイルからデータフレームを作成
        df_menu_info = self.convert_vision_response_to_dataframe()

        # １か月分のメニュー表の作成
        df_menu_for_month = self.make_menu_for_month(df_menu_info)

        # メニューの更新・通知スケジュールの追加
        df_date = (
            df_menu_for_month.select(
                date=pl.col("date")
                .str.replace(r"\s.*", "")
                .str.strptime(pl.Date, "%Y-%m-%d")
            )
            .unique()
            .with_columns(weekday=pl.col("date").dt.weekday())
            .with_columns(
                is_holiday=pl.col("date").apply(lambda x: jpholiday.is_holiday(x))
            )
            .pipe(self.add_schedule, col_name="update_this_week", weekday=1)
            .pipe(self.add_schedule, col_name="update_next_week", weekday=4)
            .pipe(self.add_schedule, col_name="notice_check_lunch", weekday=4)
            .pipe(self.add_schedule, col_name="report_next_week", weekday=4)
        )

        # １か月分のメニュー表に更新・通知スケジュールを追加
        df_menu_for_month = df_menu_for_month.join(df_date, on="date")

        # メニュー表をGoogle Driveにアップロード
        self.create_menu_to_drive(
            df=df_menu_for_month,
            drive_folder_id=self.google_drive_info["FOLDER_EXCEL"],
        )

    def add_schedule(
        self, df: pl.DataFrame, col_name: str, weekday: int
    ) -> pl.DataFrame:
        """メニューの更新・通知スケジュールの追加

        Args:
            df (pl.DataFrame): メニュー表
            col_name (str): スケジュール名
            weekday (int): 曜日(月曜日が1)

        Returns:
            pl.DataFrame: メニューの更新・通知スケジュール
        """
        df_date = (
            df.with_columns(
                pl.when(
                    (pl.col("weekday") == weekday) & (pl.col("is_holiday") == False)
                )
                .then(True)
                .otherwise(False)
                .alias(col_name)
            )
            .with_columns(monday=pl.col("date").dt.truncate(every="1w"))
            .with_columns(count=pl.lit(0))
        )

        while weekday > 0:
            df_date = (
                df_date.with_columns(
                    pl.when(
                        (pl.col("weekday") == weekday)
                        & (pl.col("is_holiday") == False)
                        & (pl.col("count") == 0)
                    )
                    .then(True)
                    .otherwise(pl.col(col_name))
                    .alias(col_name)
                )
                .with_columns(monday=pl.col("date").dt.truncate(every="1w"))
                .select(pl.exclude("count"))
            )

            df_grouped = (
                df_date.groupby("monday", maintain_order=True)
                .agg(pl.col(col_name).sum())
                .rename({col_name: "count"})
            )

            df_date = df_date.join(df_grouped, on="monday")

            weekday -= 1

        return df_date.select(pl.exclude("monday", "count"))

    def create_menu_to_drive(self, df: pl.DataFrame, drive_folder_id: str) -> None:
        """メニュー表のデータフレームをGoogle Driveに保存

        Args:
            df (pl.DataFrame): メニュー表のデータフレーム
            drive_folder_id (str): Google DriveのフォルダーID
        """
        # データフレームをCSVファイルに保存
        file_name = self.create_menu_csv(df=df)

        # スレッドシートをGoogle Driveにアップロード
        self.create_spreadsheet(file_name=file_name, folder_id=drive_folder_id)

        print(f"Upload {file_name} to Google Drive.")

    def create_menu_csv(self, df: pl.DataFrame) -> str:
        # メニューの通知・更新日を追加
        df

        # ファイル名の作成
        target_month = df["date"][0] + timedelta(weeks=1)
        file_name = f"{target_month.year}{target_month.month:02d}"
        csv_file = f"{file_name}.csv"

        # データフレームをCSVファイルに保存
        df.write_csv(file=f"./{csv_file}")

        return file_name

    def create_spreadsheet(self, file_name: str, folder_id: str) -> None:
        """ローカルのCSVファイルをスプレッドシートに変換してGoogle Driveにアップロード

        Args:
            file_name (str): CSVのファイル名
            parent_id (str): スプレッドシートを保存するGoogle DriveのフォルダのID
        """
        file_metadata = {
            "name": file_name,
            "parents": [folder_id],
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        media = MediaFileUpload(
            f"./{file_name}.csv", mimetype="text/csv", resumable=True
        )
        request = self.service_drive.files().create(
            body=file_metadata, media_body=media, fields="id", supportsAllDrives=True
        )

        done = False
        while done is False:
            _, done = request.next_chunk()

    # ----------------------------- Cloud Vision AIのOCR機能を使ってPDFからメニュー表のデータを作成 ----------------------------- #

    def convert_vision_response_to_dataframe(self) -> pl.DataFrame:
        """Cloud Vision AIで取得した文字情報をデータフレームに変換

        Args:
            gcs_destination_uri (str): _description_

        Returns:
            pl.DataFrame: _description_
        """
        # GCSに保存されたCloud Visionのレスポンスを読み込み
        response = self.read_vision_response()

        # レスポンスをデータフレームに変換
        df = self.response_to_dataframe(response["responses"][0]["fullTextAnnotation"])

        return df

    def read_vision_response(self) -> Dict[str, str]:
        """Cloud Vision AIで取得した文字情報を読み込み

        Returns:
            Dict: Cloud Vision AIで取得した文字情報
        """
        # バケットからファイル名を取得
        blob_list = [
            blob
            for blob in list(self.bucket.list_blobs(prefix="json"))
            if not blob.name.endswith("/")
        ]

        # GCSからの最初の出力ファイルを処理
        output = blob_list[0]

        json_string = output.download_as_string()

        return json.loads(json_string)

    def response_to_dataframe(self, document: Dict) -> pl.DataFrame:
        """Cloud Vision AIで取得した文字情報をデータフレームに変換

        Args:
            document (Dict): Cloud Vision AIで取得した文字情報

        Returns:
            pl.DataFrame: Cloud Vision AIで取得した文字情報のデータフレーム
        """
        # レスポンスから単語(words)と段落ごとの座標を抽出
        bounds_word = []
        words = []
        for page in document["pages"]:
            for block in page["blocks"]:
                for paragraph in block["paragraphs"]:
                    for word in paragraph["words"]:
                        word_tmp = []
                        for symbol in word["symbols"]:
                            word_tmp.append(symbol["text"])
                        bounds_word.append(word["boundingBox"])
                        word_tmp = "".join(word_tmp)
                        words.append(word_tmp)

        # 文字(text), 左下の座標x, y, 高さ(height)をデータフレームにまとめる
        left_bottom = []
        heights = []
        for bound in bounds_word:
            temp_xs = []
            temp_ys = []
            for vertice in bound["normalizedVertices"]:
                temp_xs.append(vertice["x"])
                temp_ys.append(vertice["y"])
            left_bottom.append({"x": min(temp_xs), "y": max(temp_ys)})
            heights.append(int(max(temp_ys) - min(temp_ys)))

        # 文字情報を文字列、左下のx座標、左下のy座標、高さのデータフレームにまとめる
        output_df = pl.DataFrame(
            {
                "text": text,
                "left_bottom_x": vertic["x"],
                "left_bottom_y": vertic["y"],
                "height": height,
            }
            for (text, vertic, height) in zip(words, left_bottom, heights)
        )

        return output_df

    def make_menu_for_month(self, input_df: pl.DataFrame) -> pl.DataFrame:
        """1か月分のメニュー表の作成

        Args:
            input_df (pl.DataFrame): Cloud Vision AIから取得した文字情報

        Returns:
            pl.DataFrame: 1か月分のメニュー表
        """
        start_date = self.get_start_date(input_df)

        strat_x = 0.02
        strat_y = 0.16

        output_df = pl.concat(
            [
                self.make_menu_for_week(
                    input_df=input_df,
                    left_bottom_x=strat_x,
                    left_bottom_y=strat_y,
                    date=start_date,
                ),
                self.make_menu_for_week(
                    input_df=input_df,
                    left_bottom_x=strat_x,
                    left_bottom_y=strat_y + 0.28,
                    date=start_date + timedelta(days=7),
                ),
                self.make_menu_for_week(
                    input_df=input_df,
                    left_bottom_x=strat_x,
                    left_bottom_y=strat_y + 0.56,
                    date=start_date + timedelta(days=14),
                ),
                self.make_menu_for_week(
                    input_df=input_df,
                    left_bottom_x=strat_x,
                    left_bottom_y=strat_y + 0.28,
                    date=start_date + timedelta(days=21),
                ),
                self.make_menu_for_week(
                    input_df=input_df,
                    left_bottom_x=strat_x,
                    left_bottom_y=strat_y + 0.56,
                    date=start_date + timedelta(days=28),
                ),
            ]
        )

        return output_df

    def make_menu_for_week(
        self,
        input_df: pl.DataFrame,
        left_bottom_x: float,
        left_bottom_y: float,
        date: date,
    ) -> pl.DataFrame:
        """一週間分のメニュー表の作成

        Args:
            input_df (pl.DataFrame): Cloud Vision AIから取得した文字情報
            left_bottom_x (float): 文字列の左下のx座標
            left_bottom_y (float): 文字列の左下のy座標
            date (date): メニューが記載されている日付

        Returns:
            pl.DataFrame: 一週間分のメニュー表
        """
        output_df = pl.concat(
            [
                self.make_menu_for_oneday(input_df, left_bottom_x, left_bottom_y, date),
                self.make_menu_for_oneday(
                    input_df,
                    left_bottom_x + 0.19,
                    left_bottom_y,
                    date + timedelta(days=1),
                ),
                self.make_menu_for_oneday(
                    input_df,
                    left_bottom_x + 0.38,
                    left_bottom_y,
                    date + timedelta(days=2),
                ),
                self.make_menu_for_oneday(
                    input_df,
                    left_bottom_x + 0.57,
                    left_bottom_y,
                    date + timedelta(days=3),
                ),
                self.make_menu_for_oneday(
                    input_df,
                    left_bottom_x + 0.76,
                    left_bottom_y,
                    date + timedelta(days=4),
                ),
            ]
        )

        return output_df

    def make_menu_for_oneday(
        self,
        input_df: pl.DataFrame,
        left_bottom_x: float,
        left_bottom_y: float,
        date: date,
    ) -> pl.DataFrame:
        """一日分のメニュー表の作成

        Args:
            input_df (pl.DataFrame): Cloud Vision AIから取得した文字情報
            left_bottom_x (float): 文字列の左下のx座標
            left_bottom_y (float): 文字列の左下のy座標
            date (date): メニューが記載されている日付

        Returns:
            pl.DataFrame: 一日分のメニュー表
        """
        # 当日の一番上のメニューを取得
        top_menu = self.extract_text_from_region(
            input_df, left_bottom_x, left_bottom_y, 0.15, 0.03
        )

        # 当日の一番上のメニューが空白の場合は空のデータフレームを作成
        if top_menu == "":
            output_df = pl.DataFrame(
                schema={"date": pl.Date, "name": pl.Utf8, "price": pl.Int16}
            )
        # 空白でない場合は当日のメニュー表のデータフレームを作成
        else:
            output_df = pl.DataFrame(
                {
                    "date": date,
                    "name": [
                        self.extract_text_from_region(
                            input_df, left_bottom_x, left_bottom_y, 0.14, 0.03
                        ),
                        self.extract_text_from_region(
                            input_df, left_bottom_x, left_bottom_y + 0.024, 0.14, 0.03
                        ),
                        self.extract_text_from_region(
                            input_df, left_bottom_x, left_bottom_y + 0.048, 0.14, 0.03
                        ),
                        self.extract_text_from_region(
                            input_df, left_bottom_x, left_bottom_y + 0.072, 0.14, 0.03
                        ),
                        self.extract_text_from_region(
                            input_df, left_bottom_x, left_bottom_y + 0.096, 0.14, 0.03
                        ),
                    ],
                    "price": [
                        self.extract_text_from_region(
                            input_df, left_bottom_x + 0.17, left_bottom_y, 0.02, 0.03
                        ),
                        self.extract_text_from_region(
                            input_df,
                            left_bottom_x + 0.17,
                            left_bottom_y + 0.024,
                            0.02,
                            0.03,
                        ),
                        self.extract_text_from_region(
                            input_df,
                            left_bottom_x + 0.17,
                            left_bottom_y + 0.048,
                            0.02,
                            0.03,
                        ),
                        self.extract_text_from_region(
                            input_df,
                            left_bottom_x + 0.17,
                            left_bottom_y + 0.072,
                            0.02,
                            0.03,
                        ),
                        self.extract_text_from_region(
                            input_df,
                            left_bottom_x + 0.17,
                            left_bottom_y + 0.096,
                            0.02,
                            0.03,
                        ),
                    ],
                }
            ).with_columns(
                name=pl.col("name").str.replace(r"\|", ""),
                price=pl.col("price").str.replace(r"\|", "").cast(pl.Int16),
            )

        return output_df

    def get_start_date(self, input_df: pl.DataFrame) -> date:
        """メニュー表の最初の日付を取得

        Args:
            input_df (pl.DataFrame): Cloud Vision AIから取得した文字情報

        Returns:
            date: メニュー表の最初の日付
        """
        # メニュー表に記載されているはじめの月日
        month_day = self.extract_text_from_region(
            input_df=input_df,
            left_bottom_x=0.07,
            left_bottom_y=0.14,
            width=0.07,
            height=0.03,
        )

        # メニュー表に記載されているはじめの年月日
        year_month_day = self.make_ymd(month_day)

        return year_month_day

    def make_ymd(self, month_day: str) -> date:
        """メニュー表に記載されているはじめの年月日を作成

        Args:
            month_day (str): 月日

        Returns:
            date: メニュー表に記載されているはじめの年月日
        """
        year = str((datetime.now() + relativedelta(months=1)).year) + "年"
        return datetime.strptime(year + month_day, "%Y年%m月%d日").date()

    def extract_text_from_region(
        self,
        input_df: pl.DataFrame,
        left_bottom_x: float,
        left_bottom_y: float,
        width: float,
        height: float,
    ) -> str:
        """領域(left_bottom_x, left_bottom_y, width, height)を指定し,その領域に含まれる文字列を抽出

        Args:
            input_df (pl.DataFrame): Cloud Vision AIから取得した文字情報
            left_bottom_x (float): 文字列の左下のx座標
            left_bottom_y (float): 文字列の左下のy座標
            width (float): 指定した領域の幅
            height (float): 指定した領域の高さ

        Returns:
            str: 抽出した文字列
        """
        output_df = input_df.filter(
            (pl.col("left_bottom_x") >= left_bottom_x)
            & (pl.col("left_bottom_y") >= left_bottom_y)
            & (pl.col("left_bottom_x") <= left_bottom_x + width)
            & (pl.col("left_bottom_y") <= left_bottom_y + height)
        )

        return "".join(output_df["text"].to_list())

    # ----------------------------- Glideのメニュー表の操作 ----------------------------- #

    def update_menu_next_week(self, this_date: date) -> None:
        """来週のメニューをアップデート

        Args:
            this_date (date): 当日の日付
        """
        # Google Driveに保存されているCSVファイルからメニュー表を読み込み
        df_menu = self.read_menu_excel(this_date=this_date)

        # ユーザー情報の取得
        df_user = self.read_spreadsheet(
            sheet_id=self.google_drive_info["SPREAD_SHEET"], ranges="App: Logins!B1:B10"
        )

        # 翌週のメニュー表を作成
        df_menu_next_week = (
            df_menu.join(df_user, how="cross")
            .with_columns(date=pl.col("date").str.strptime(pl.Date, "%Y-%m-%d"))
            .with_columns(price="¥" + pl.col("price").cast(str))
            .with_columns(check=pl.lit(""))
            .with_columns(monday=pl.col("date").dt.truncate(every="1w"))
            .with_columns(diff_days=(pl.col("monday") - this_date).dt.days())
            .filter(pl.col("diff_days") > 0)
            .filter(pl.col("diff_days") == pl.col("diff_days").min())
            .filter(pl.col("is_holiday") == "FALSE")
            .select("date", "name", "price", "check", "Email")
        )

        # アプリの登録人数
        menber_num = len(df_menu_next_week.unique(subset="Email"))

        # 翌週のメニュー表をスプレッドシートに上書き
        self.write_spreadsheet(
            ranges=f"next_week!A1:E{menber_num*25+1}", df=df_menu_next_week
        )

    def update_menu_this_week(self) -> None:
        """今週のメニューをアップデート"""
        # ユーザー情報の取得
        df_user = self.read_spreadsheet(
            sheet_id=self.google_drive_info["SPREAD_SHEET"], ranges="App: Logins!B1:B10"
        )

        # アプリの登録人数
        menber_num = len(df_user.unique(subset="Email"))

        # 翌週のメニューの取得
        df_menu_next_week = self.read_spreadsheet(
            sheet_id=self.google_drive_info["SPREAD_SHEET"],
            ranges=f"next_week!A1:E{menber_num*25+1}",
        ).with_columns(
            date=pl.col("date")
            .str.replace(r"\s.*", "")
            .str.strptime(pl.Date, "%Y-%m-%d")
        )

        # 注文されたメニューを抽出
        df_menu_this_week = df_menu_next_week.filter(pl.col("check") == "TRUE").select(
            pl.exclude("check")
        )

        # 今週のメニューをスプレッドシートに上書き
        self.write_spreadsheet(
            ranges=f"this_week!A1:D{menber_num*25+1}", df=df_menu_this_week
        )

    def report_menu_next_week(self) -> None:
        """来週のメニューをslackにレポート"""
        # ユーザー情報の取得
        df_user = self.read_spreadsheet(
            sheet_id=self.google_drive_info["SPREAD_SHEET"], ranges="App: Logins!B1:B10"
        )

        # アプリの登録人数
        menber_num = len(df_user.unique(subset="Email"))

        # 翌週のメニューの取得
        df_menu_next_week = self.read_spreadsheet(
            sheet_id=self.google_drive_info["SPREAD_SHEET"],
            ranges=f"next_week!A1:E{menber_num*25+1}",
        ).with_columns(
            date=pl.col("date")
            .str.replace(r"\s.*", "")
            .str.strptime(pl.Date, "%Y-%m-%d")
        )

        # 翌週のメニューの集計
        df_menu_summary = (
            df_menu_next_week.filter(pl.col("check") == "TRUE")
            .select("date", "name", "price", "check")
            .groupby(["date", "name", "price"])
            .count()
            .sort(["date"])
        )

        # 翌週のメニューをslackに送信
        self.message_to_slack(
            channel_name="sapporo_lunch",
            header_text="来週のお弁当は下記の通りです:point_down:",
            df=df_menu_summary,
        )

    def read_menu_excel(self, this_date: date) -> pl.DataFrame:
        """Google Driveに保存されているEXCELファイルからメニュー表を読み込み

        Args:
            this_date (date): 当日の日付

        Returns:
            pl.DataFrame: メニュー表
        """
        # Google Driveに保存されているCSVファイルの検索
        xlsxs = self.search_drive_files(
            folder_id=self.google_drive_info["FOLDER_EXCEL"],
            file_type="",
            search_date=self.get_pastday(this_date=this_date, days=31),
        )

        # Google DriveからCSVファイルをダウンロード
        df = pl.DataFrame()
        for i in range(2):
            xlsx = xlsxs[i]
            df_menu = self.read_spreadsheet(
                sheet_id=xlsx["id"], ranges=f"{xlsx['name']}!A1:I126"
            )
            df = pl.concat([df, df_menu])

        return df.sort(["date"]).unique()

    def read_spreadsheet(self, sheet_id: str, ranges: str) -> pl.DataFrame:
        """Google Driveに保存されているスプレッドシートからデータを読み込み

        Args:
            sheet_id (str): スプレッドシートのID
            ranges (str): スプレッドシートのシート名:セル範囲

        Returns:
            pl.DataFrame: スプレッドシートから取得したデータ
        """
        # リクエスト
        response = (
            self.service_sheets.spreadsheets()
            .values()
            .batchGet(
                spreadsheetId=sheet_id,
                ranges=[ranges],
            )
            .execute()
        )

        # レスポンスからデータ部分の抽出
        ranges = response.get("valueRanges", [])

        return pl.DataFrame(ranges[0]["values"][1:], schema=ranges[0]["values"][0])

    def write_spreadsheet(self, ranges: str, df: pl.DataFrame) -> None:
        """データフレームをスプレッドシートに書き込み

        Args:
            ranges (str): スプレッドシートのシート名:セル範囲
            df (pl.DataFrame): 書き込むデータ
        """
        # スプレッドシートのデータを削除
        self.remove_spreadsheet(ranges=ranges)

        # スプレッドシートに書き込むデータ
        data = [
            {
                "range": ranges,
                "majorDimension": "COLUMNS",
                "values": [
                    [col] + df[col].dt.strftime("%Y-%m-%d").to_list()
                    if col == "date"
                    else [col] + df[col].to_list()
                    for col in df.columns
                ],
            }
        ]

        # リクエスト
        (
            self.service_sheets.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=self.google_drive_info["SPREAD_SHEET"],
                body={"value_input_option": "USER_ENTERED", "data": data},
            )
            .execute()
        )

    def remove_spreadsheet(self, ranges: str) -> None:
        """特定のセル範囲に書かれたスプレッドシートのデータを削除

        Args:
            ranges (str): スプレッドシートのシート名:セル範囲
        """
        # リクエスト
        (
            self.service_sheets.spreadsheets()
            .values()
            .clear(
                spreadsheetId=self.google_drive_info["SPREAD_SHEET"],
                body={},
                range=ranges,
            )
            .execute()
        )

    # ----------------------------- Slackにメッセージを送信 ----------------------------- #

    def message_to_slack(
        self,
        channel_name: str,
        header_text: str,
        body_text: str = None,
        df: pl.DataFrame = None,
    ) -> None:
        """slackにメッセージを送信

        Args:
            channel_name (str): チャンネル名
            header_text (str): ヘッダー文
            body_text (str, optional): 本文. Defaults to None.
            df (pl.DataFrame, optional): メニュー表. Defaults to None.
        """
        client = WebClient(token=self.slack_info["SLACK_TOKEN"])

        try:
            response = client.chat_postMessage(
                channel=self.slack_info["CHANNEL_ID"][channel_name],
                blocks=self.make_slack_blocks(
                    header_text=header_text, body_text=body_text, df=df
                ),
            )
        except SlackApiError as e:
            assert e.response["error"]

    def make_slack_blocks(
        self, header_text: str, body_text: str = None, df: pl.DataFrame = None
    ) -> List[Dict[str, Any]]:
        """slackに送信するメッセージブロックを作成

        Args:
            header_text (str): ヘッダー文
            body_text (str, optional): 本文. Defaults to None.
            df (pl.DataFrame, optional): メニュー表. Defaults to None.

        Returns:
            List[Dict[str, Any]]: slackに送信するメッセージ
        """
        messages = [
            self.make_slack_block(
                type="header", sub_type="plain_text", text=header_text
            )
        ]
        if body_text is not None:
            messages.append(
                self.make_slack_block(type="section", sub_type="mrkdwn", text=body_text)
            )
        if df is not None:
            df = df.with_columns(date=pl.col("date").dt.strftime("%Y年%m月%d日 (%a)"))
            for menu in df.iter_rows(named=True):
                messages.append(
                    self.make_slack_block(
                        type="section",
                        sub_type="mrkdwn",
                        text=f"{menu['date']} {menu['name']} {menu['price']} {menu['count']}個",
                    )
                )

        return messages

    def make_slack_block(self, type: str, sub_type: str, text: str) -> Dict[str, Any]:
        """slackに送信するメッセージブロックを作成

        Args:
            type (str): コンポーネントのタイプ
            sub_type (str): textのタイプ
            text (str): メッセージの文章

        Returns:
            Dict[str, Any]: slackに送信する1ブロック
        """
        return {"type": type, "text": {"type": sub_type, "text": text}}
