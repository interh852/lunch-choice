import os
import io
import json
import polars as pl
from typing import Dict, List
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import google.auth
from google.cloud import vision
from google.cloud import storage
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


class MenuList:
    def __init__(self):
        self.service = build(
            serviceName="drive", version="v3", credentials=google.auth.default()[0]
        )
        self.client = storage.Client()
        self.bucket_name = "lunch-choice"
        self.bucket = self.client.bucket(self.bucket_name)
        self.google_drive_info = self.read_google_drive_info()

    def read_google_drive_info(self) -> Dict[str, str]:
        """GCSに保存されているGoogle Driveの情報を読み込み

        Returns:
            Dict[str, str]: Google Driveの情報
        """
        blob = self.bucket.blob(blob_name="drive/google_drive.json")
        google_drive_path = json.load(io.BytesIO(blob.download_as_bytes()))

        return google_drive_path

    def create_menu_csv(self) -> None:
        """メニュー表をPDFからCSVに変換しGoogle Driveに保存"""
        # Google Driveに新たに追加されたPDFファイルを検索
        pdfs = self.search_drive_pdf(
            drive_pdf_folder_id=self.google_drive_info["FOLDER_PDF"]
        )

        if pdfs:
            for pdf in pdfs:
                # 新たに追加されたPDFファイルをGCSにコピー
                self.copy_menu_from_drive_to_gcs(pdf_info=pdf)

                # PDFから文字情報のjSONファイルを取得
                self.async_detect_document(
                    gcs_source_uri=f"gs://{self.bucket_name}/pdf/{pdf['name']}",
                    gcs_destination_uri=f"gs://{self.bucket_name}/json/",
                )

                # 文字情報のJSONファイルをメニュー表のCSVに変換しGoogle Driveに保存
                self.convert_menu_csv()

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

        # ローカルのPDFを削除
        os.remove(pdf_info["name"])

        print(f"Upload {pdf_info['name']} to GCS")

    def search_drive_pdf(self, drive_pdf_folder_id: str) -> List[Dict[str, str]]:
        """Google DriveからPDFファイルを検索

        Args:
            folder_id (str): メニュメモのフォルダID

        Returns:
            List[str]: PDFファイルのリスト
        """
        # 検索条件
        condition_list = [
            f"('{drive_pdf_folder_id}' in parents)",
            "(name contains '.pdf')",
            f"(createdTime >= '{self.get_yesterday()}')",
        ]
        conditions = " and ".join(condition_list)

        # フォルダ内を検索
        results = (
            self.service.files()
            .list(
                q=conditions,
                fields="nextPageToken, files(id, name)",
                includeItemsFromAllDrives=True,
                supportsAllDrives=True,
            )
            .execute()
        )
        files = results.get("files", [])

        return files

    def get_yesterday(self) -> str:
        """前日の日付を取得

        Returns:
            str: 昨日の日付
        """
        return (date.today() - timedelta(days=1)).isoformat()

    def download_drive_file(self, file_id: str, filename: str) -> None:
        """Google DriveからPDFのダウンロード

        Args:
            pdf_id (str): Google DriveにあるPDFファイルのID
        """
        request = self.service.files().get_media(fileId=file_id)
        file = io.FileIO(filename, "wb")
        downloader = MediaIoBaseDownload(file, request)

        done = False
        while done is False:
            _, done = downloader.next_chunk()
            # print(f"Download {int(status.progress() * 100)}.")

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

    def convert_menu_csv(self) -> None:
        """文字情報のJSONファイルからメニュー表のCSVファイルに変換しGoogle Driveに保存"""
        # 文字情報のJSONファイルからデータフレームを作成
        df_menu_info = self.convert_vision_response_to_dataframe()

        # １か月分のメニュー表の作成
        df_menu_for_month = self.make_menu_for_month(df_menu_info)

        # メニュー表をGoogle Driveにアップロード
        self.upload_menu_to_drive(
            df=df_menu_for_month,
            drive_csv_folder_id=self.google_drive_info["FOLDER_CSV"],
        )

    def upload_menu_to_drive(self, df: pl.DataFrame, drive_csv_folder_id: str) -> None:
        """メニュー表のデータフレームをGoogle Driveに保存

        Args:
            df (pl.DataFrame): メニュー表のデータフレーム
            drive_csv_folder_id (str): Google DriveのフォルダーID
        """
        # データフレームをCSVファイルに保存
        target_month = date.today() + relativedelta(months=1)
        csv_file = f"{target_month.year}{target_month.month:02d}.csv"
        df.write_csv(file=f"./{csv_file}")

        # CSVファイルをGoogle Driveにアップロード
        self.upload_drive_csv(file_name=csv_file, folder_id=drive_csv_folder_id)

        # CSVファイルの削除
        os.remove(csv_file)

        print(f"Upload {csv_file} to Google Drive.")

    def upload_drive_csv(self, file_name: str, folder_id: str) -> None:
        """ローカルのCSVファイルをGoogle Driveにアップロード

        Args:
            file_name (str): CSVのファイル名
            parent_id (str): CSVファイルを保存するGoogle DriveのフォルダのID
        """
        file_metadata = {"name": file_name, "parents": [folder_id]}
        media = MediaFileUpload(
            f"./{file_name}", mimetype="application/csv", resumable=True
        )
        request = self.service.files().create(
            body=file_metadata, media_body=media, supportsAllDrives=True
        )

        done = False
        while done is False:
            _, done = request.next_chunk()
            # print(f"Uploaded {int(status.progress() * 100)}.")

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
        month_day = self.extract_text_from_region(
            input_df=input_df,
            left_bottom_x=0.07,
            left_bottom_y=0.14,
            width=0.07,
            height=0.03,
        )

        return self.make_ymd(month_day)

    def make_ymd(self, month_day: str) -> date:
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
        output_df = input_df.filter(
            (pl.col("left_bottom_x") >= left_bottom_x)
            & (pl.col("left_bottom_y") >= left_bottom_y)
            & (pl.col("left_bottom_x") <= left_bottom_x + width)
            & (pl.col("left_bottom_y") <= left_bottom_y + height)
        )

        return "".join(output_df["text"].to_list())
