/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison interface for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2011 Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     CREATE = 258,
     TABLE = 259,
     INSERT = 260,
     INTO = 261,
     VALUES = 262,
     DROP = 263,
     SELECT = 264,
     DISTINCT = 265,
     DELETE = 266,
     FROM = 267,
     WHERE = 268,
     ORDER = 269,
     BY = 270,
     AND = 271,
     OR = 272,
     NOT = 273,
     OPAREN = 274,
     EPAREN = 275,
     OPAREN_SQ = 276,
     EPAREN_SQ = 277,
     COMMA = 278,
     STAR = 279,
     PLUS = 280,
     MINUS = 281,
     DIV = 282,
     LT = 283,
     GT = 284,
     EQ = 285,
     NULLTOK = 286,
     NEWLINE = 287,
     NAME = 288,
     INTTOK = 289,
     STR20TOK = 290,
     INTEGER = 291,
     LITERAL = 292,
     DOT_PREFIX_NAME = 293
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{

/* Line 2068 of yacc.c  */
#line 886 "TinySQL.y"

    int number;
    char *str;
  


/* Line 2068 of yacc.c  */
#line 95 "TinySQL.hh"
} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE yylval;


