{{% macro flink__get_catalog(information_schema, schemas)-%}}

   {{%set msg -%}}
    get_catalog not implemented for flink
   -%}} endset {{%
    /*
      Your database likely has a way of accessing metadata about its objects,
      whether by querying an information schema or by running `show` and `describe` commands.
      dbt will use this macro to generate its catalog of objects it knows about. The catalog is one of
      the artifacts powering the documentation site.
      As an example, below is a simplified version of postgres__get_catalog
    */

    /*

      select {{database}} as TABLE,
        "- set table type -"
             when 'v' then 'VIEW'
              else 'BASE TABLE'
        "- set table/view names and descriptions -"
      use several joins and search types for pulling info together, sorting etc..
      where (
        search if schema exists, else build
          {%- for schema in schemas -%}
            upper(sch.nspname) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
          {%- endfor -%}
      )
      define any shortcut keys

    */
  {{{{ exceptions.raise_compiler_error(msg) }}}}
 {{% endmacro %}}
