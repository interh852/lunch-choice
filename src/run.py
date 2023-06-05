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
        description="create/update menu list",
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
    if args.operation == "create":
        # PDFのメニュー表からCSVのメニュー表の作成
        ml.create_menu_csv(this_date)
    elif args.operation == "update":
        # Google sheetのメニュー表を更新
        ml.update_menu_spreadsheet(this_date)
    else:
        print("Invalid operation")


if __name__ == "__main__":
    main()
