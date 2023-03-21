create_catalog = [
    """
    {
        "results": {
            "columns": [
                {
                    "name": "result",
                    "logicalType": {
                        "type": "VARCHAR",
                        "nullable": true,
                        "length": 2147483647
                    },
                    "comment": null
                }
            ],
            "data": [
                {
                    "kind": "INSERT",
                    "fields": [
                        "OK"
                    ]
                }
            ]
        },
        "resultType": "PAYLOAD",
        "nextResultUri": "/v1/sessions/_session/operations/_operation/result/1"
    }
    """.strip()
    ,
    """
    {
        "results": {
            "columns": [
                {
                    "name": "result",
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
    }
    """.strip()
]
