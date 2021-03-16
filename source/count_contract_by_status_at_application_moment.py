data_posh_balance\
.filter( data_posh_balance['MONTHS_BALANCE'] == -1 )\
.groupBy(["SK_ID_CURR"])\
.pivot('NAME_CONTRACT_STATUS')\
.agg({"NAME_CONTRACT_STATUS":"count"})\
.toPandas()
