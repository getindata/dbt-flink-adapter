show_views = [
    """
{
    "results": {
        "columns": [
            {
                "name": "view name",
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
                    "raw_orders2_view"
                ]
            },
            {
                "kind": "INSERT",
                "fields": [
                    "v99"
                ]
            }
        ]
    },
    "resultType": "PAYLOAD",
    "nextResultUri": "/v1/sessions/_session/operations/_operation/result/1"
}""".strip(),

    """
{
    "results": {
        "columns": [
            {
                "name": "view name",
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
