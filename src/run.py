import argparse
from menu_list import MenuList


def get_option() -> argparse.Namespace:
    """コマンドライン引数の取得

    Returns:
        argparse.Namespace: コマンドライン引数
    """
    argparser = argparse.ArgumentParser(
        prog="run", usage="run.py -o operation", description="create/update menu list"
    )
    argparser.add_argument(
        "-o", "--operation", type=str, help="operation", default="create"
    )

    return argparser.parse_args()


def main():
    # 入力引数の取得
    args = get_option()

    ml = MenuList()
    if args.operation == "create":
        # PDFのメニュー表からCSVのメニュー表の作成
        ml.create_menu_csv()
    elif args.operation == "update":
        ml.update_menu()
    else:
        print("Invalid operation")


if __name__ == "__main__":
    main()
