# CoinMetrics.io analytics scripts

This repo contains a few scripts for blockchain analytics used by [CoinMetrics.io](https://coinmetrics.io/).

## Status

This repo is by no means complete. Also CoinMetrics systems are not transitioned yet to use the exact versions of the scripts published in this repository (but will be soon). Therefore scripts released here may differ from actual scripts running on CoinMetrics servers and may be not fully tested. Please use with caution.

## Usage

These scripts are to be used with PostgreSQL database containing raw information from blockchain, produced by running [`coinmetrics-export` tool](https://github.com/coinmetrics-io/haskell-tools).

## Contents

* `data` - so far contains information about selected set of ERC20 tokens, suitable for `coinmetrics-export export-erc20-info` command.
* `scripts` - scripts for refreshing information in database and generating CSV files.
* `sql` - SQL scripts for creating materialized views for analytics.
