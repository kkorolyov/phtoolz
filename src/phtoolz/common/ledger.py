"""Provides ledger data."""

import csv
import re
import subprocess
from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from typing import NamedTuple

from phtoolz.common.commodity import CommodityValue


class Transaction(NamedTuple):
    """A change in quantity of a commodity in an account at some time."""

    time: date
    account: str
    commodity: str
    quantity: Decimal
    basis: Decimal


class Stats(NamedTuple):
    """Ledger statistics."""

    start: date
    end: date


class Ledger:
    """Returns ledger data from a given file."""

    path: str

    def __init__(self, path: str) -> None:
        """Returns a ledger reading from file at `path`."""

        self.path = path

    def accounts(self) -> list[str]:
        """Returns all the accounts in the ledger."""

        args = ["hledger", "accounts"]
        if self.path:
            args.extend(["-f", self.path])

        return subprocess.check_output(args).decode().splitlines()

    def commodities(self) -> list[str]:
        """Returns all the commodities in the ledger."""

        args = ["hledger", "commodities"]
        if self.path:
            args.extend(["-f", self.path])

        return subprocess.check_output(args).decode().splitlines()

    def prices(self, infer: bool = False) -> list[CommodityValue]:
        """Returns all commodity prices (optionally `infer`red) in the ledger."""

        args = ["hledger", "prices"]
        if self.path:
            args.extend(["-f", self.path])
        if infer:
            args.append("--infer-market-prices")

        reader = csv.reader(
            subprocess.check_output(args).decode().splitlines(),
            delimiter=" ",
        )

        return list(
            {
                # keep only the last value of a (commodity, time) combo
                (t.name, t.time): t
                for t in (
                    CommodityValue(
                        date.fromisoformat(line[1]),
                        line[2],
                        Decimal(line[3].replace(",", "")),
                    )
                    for line in reader
                )
            }.values()
        )

    def transactions(self, forecastOnly: bool = False) -> list[Transaction]:
        """Returns transactions (optionally `forecastOnly`) from ledger."""

        # combine transactions with same (account, date, commodity)
        transactions = dict[tuple[str, date, str], Transaction]()
        for line, basisLine in zip(
            self._registerReader(forecastOnly),
            self._registerReader(forecastOnly, True),
            strict=True,
        ):
            time = date.fromisoformat(line[1])
            account = line[4]

            quantityCommodity = line[5]
            basisCommodity = basisLine[5]
            if " " in quantityCommodity:
                splitI = quantityCommodity.index(" ")
                quantity = Decimal(quantityCommodity[:splitI])
                commodity = quantityCommodity[(splitI + 1) :].replace('"', "")
            else:
                quantity = Decimal(quantityCommodity)
                commodity = "USD"

            basis = Decimal(basisCommodity)

            key = (account, time, commodity)
            if key in transactions:
                current = transactions[key]
                transactions[key] = Transaction(
                    time,
                    account,
                    commodity,
                    current.quantity + quantity,
                    current.basis + basis,
                )
            else:
                transactions[key] = Transaction(
                    time, account, commodity, quantity, basis
                )

        return list(transactions.values())

    def _registerReader(
        self,
        forecastOnly: bool = False,
        basis: bool = False,
    ) -> Iterable[tuple[str, str, str, str, str, str, str]]:
        """Returns a CSV reader over transactions in format (txnidx date code description account amount total)"""
        args = ["hledger", "register", "-O", "tsv"]
        if self.path:
            args.extend(("-f", self.path))
        if forecastOnly:
            args.extend(("--forecast=2010..", "tag:generated"))
        if basis:
            args.append("-B")

        # returns in format (txnidx date code description account amount total)
        reader = csv.reader(
            subprocess.check_output(args).decode().splitlines(),
            delimiter="\t",
        )

        # skip headers
        next(reader)

        return reader  # pyright: ignore[reportReturnType]

    def stats(self) -> Stats:
        """Returns ledger statistics."""

        args = ["hledger", "stats"]
        if self.path:
            args.extend(["-f", self.path])

        reader = csv.reader(
            subprocess.check_output(args).decode().splitlines(),
            delimiter=":",
        )
        start, end = (
            date.fromisoformat(t)
            for t in re.compile(r"\d{4}-\d{2}-\d{2}").findall(
                next(
                    line[1].strip()
                    for line in reader
                    if line[0].startswith("Txns span")
                )
            )
        )

        return Stats(start, end)
