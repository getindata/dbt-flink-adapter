show_current_catalog = [
    """
    {
        "results": {
            "columns": [
                {
                    "name": "current catalog name",
                    "logicalType": {
                        "type": "VARCHAR",
                        "nullable": true,
                        "length": 2147483647
                    },
                    "comment": null
                }
            ],
            "data": []
        },
        "resultType": "PAYLOAD",
        "nextResultUri": "/v1/sessions/_session/operations/_operation/result/1"
    }""".strip(),

    """
    {
        "results": {
            "columns": [
                {
                    "name": "current catalog name",
                    "logicalType": {
                        "type": "VARCHAR",
                        "nullable": true,
                        "length": 2147483647
                    },
                    "comment": null
                }
            ],
            "data": []
        },
        "resultType": "EOS",
        "nextResultUri": null
    }""".strip()
]
