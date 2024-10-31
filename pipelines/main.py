from to_raw.c6_bank.credit import load_credit_to_raw
from to_raw.c6_bank.debit import load_debit_to_raw
from to_clean.c6_bank.credit import load_credit_to_clean
from to_clean.c6_bank.debit import load_debit_to_clean


def main():
    # C6 Credit Pipeline
    load_credit_to_raw()
    load_credit_to_clean()

    # C6 Debit Pipeline
    load_debit_to_raw()
    load_debit_to_clean()


if __name__ == "__main__":
    main()
