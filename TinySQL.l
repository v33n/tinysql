%{
  #include <stdio.h>
  #include <string.h>
  #include "TinySQL.hh"
  extern YYSTYPE yylval;
%}

NAME [a-zA-Z][a-zA-Z0-9]*

DOT_PREFIX_NAME [a-zA-Z][a-zA-Z0-9]*[.][a-zA-Z][a-zA-Z0-9]*

LITERAL \"[^\"]\"

INTEGER [0-9]+
%%

CREATE printf("CREATE "); return CREATE;

TABLE printf("TABLE "); return TABLE;

INSERT printf("INSERT "); return INSERT;

INTO printf("INTO "); return INTO;

VALUES printf("VALUES "); return VALUES;

DROP printf("DROP "); return DROP;

SELECT printf("SELECT "); return SELECT;

DISTINCT printf("DISTINCT "); return DISTINCT;

DELETE printf("DELETE "); return DELETE;

FROM printf("FROM "); return FROM;

WHERE printf("WHERE "); return WHERE;

AND printf("AND "); return AND;

OR printf("OR "); return OR;

NOT printf("NOT "); return NOT;

ORDER printf("ORDER "); return ORDER;

BY printf("BY "); return BY;

INT printf("INT "); yylval.number=0; return INTTOK;

STR20 printf("STR20 "); yylval.number=1; return STR20TOK;

NULL printf("NULL "); return NULLTOK;

\( printf("( "); return OPAREN;

\) printf(") "); return EPAREN;

\[ printf("[ "); return OPAREN_SQ;

\] printf("] "); return EPAREN_SQ;

,  printf(", "); return COMMA;

\*  printf("* "); return STAR;

\+  printf("+ "); return PLUS;

\-  printf("- "); return MINUS;

\/  printf("/ "); return DIV;

\<  printf("< "); return LT;

\>  printf("> "); return GT;

\=  printf("= "); return EQ;

{NAME} yylval.str=strdup(yytext); printf("%s ", yytext); return NAME;

{DOT_PREFIX_NAME} yylval.str=strdup(yytext); printf("%s ", yytext); return DOT_PREFIX_NAME;

{LITERAL} yylval.str=strdup(yytext); printf("%s ", yytext); return LITERAL;

{INTEGER} yylval.str=strdup(yytext); printf("%s ", yytext); return INTEGER;
 
\n                      printf("\n"); return NEWLINE;

[ \t]+                  /* ignore whitespace */;
%%