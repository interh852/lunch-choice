import argparse
from datetime import date, datetime
from menu_list import MenuList


def get_option() -> argparse.Namespace:
    """コマンドライン引数の取得

    Returns:
        argparse.Namespace: コマンドライン引数
    """
    argparser = argparse.ArgumentParser(
        prog="run",
        usage="run.py -d date -o operation",
        description="create or update menu list,and message to slack",
    )
    argparser.add_argument(
        "-d", "--date", type=str, help="date", default=date.today().isoformat()
    )
    argparser.add_argument(
        "-o", "--operation", type=str, help="operation", default="create"
    )

    return argparser.parse_args()


def main():
    # 入力引数の取得
    args = get_option()

    # 日付
    this_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    ml = MenuList()
    is_check_execute = ml.check_execute(operation=args.operation, this_date=this_date)
    if is_check_execute:
        if args.operation == "create":
            # PDFのメニュー表からスプレッドシートのメニュー表の作成
            ml.create_menu_spreadsheet(this_date)
        elif args.operation == "update_next_week":
            # Google sheetの来週のメニュー表を更新
            ml.update_menu_next_week(this_date)
        elif args.operation == "notice_check_lunch":
            # 来週のお弁当チェックの通知
            ml.message_to_slack(
                channel_name="sapporo_lunch",
                header_text="来週のお弁当のチェックをお願いします:white_check_mark:",
                body_text=f":iphone: {ml.google_drive_info['APP_URL']}",
            )
        elif args.operation == "update_this_week":
            # Google sheetの今週のメニュー表を更新
            ml.update_menu_this_week()
        elif args.operation == "report_next_week":
            # 来週のお弁当の注文リストを集計してレポート
            ml.report_menu_next_week()
        else:
            print("Invalid operation")


if __name__ == "__main__":
    main()
