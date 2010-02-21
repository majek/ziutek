#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
>>> parse("aaa".split())
['aaa']
>>> parse("aaa AND".split())
Traceback (most recent call last):
 ...
ValueError: Unexpected token: (None, 'EOF')
>>> parse("aaa bbb".split())
Traceback (most recent call last):
 ...
ValueError: Expected token of kind EOF, got: bbb
>>> parse("aaa AND bbb ANDNOT ccc".split())
['aaa', 'bbb', 'AND', 'ccc', 'ANDNOT']
>>> parse("aaa AND ( bbb ANDNOT ccc )".split())
['aaa', 'bbb', 'ccc', 'ANDNOT', 'AND']
>>> parse("vvv AND xxx OR yyy ANDNOT zzz OR ccc AND aaa".split())
['vvv', 'xxx', 'yyy', 'OR', 'AND', 'zzz', 'ccc', 'OR', 'ANDNOT', 'aaa', 'AND']
'''


EOF = 'EOF'
KEYWORD = 'K'
LEFT_PAREN = '('
RIGHT_PAREN = ')'
OP = 'OP'
UNARY = 'UN'

LEFT = 'L'
RIGHT = 'R'

operators = { # highest priority binds stronger
    # 'priority', 'associativity'
    'AND':      (1, LEFT),
    'OR':       (2, LEFT),
    'ANDNOT':   (1, LEFT),
}

def priority(op):
    return operators[op][0]

def associativity(op):
    return operators[op][1]

class ReadAhead:
    def __init__(self, token_source):
        self._source = token_source
        self._next = None

    def _read(self):
        try:
            t = self._source.next()
            t, kind = classify_token(t)
        except StopIteration:
            t, kind = None, EOF
        return (t, kind)

    def next_token(self):
        if self._next is None:
            self._next = self._read()
        return self._next

    def consume_token(self):
        # print "Consummed %r" % (self._next,)
        self._next = None

    def expect(self, expected_kind):
        t, kind = self.next_token()
        if kind == expected_kind:
            self.consume_token()
        else:
            raise ValueError, "Expected token of kind %s, got: %s" % (expected_kind, t)
        
def classify_token(t):
    if t == '(':
        kind = LEFT_PAREN
    elif t == ')':
        kind = RIGHT_PAREN
    elif t in operators.keys():
        kind = OP
    else:
        kind = KEYWORD
    return (t, kind)


"""
Precedense climbing parser.
see: http://www.engr.mun.ca/~theo/Misc/exp_parsing.htm

grammar:

E --> Exp(0) 
Exp(p) --> P {B Exp(q)} 
P --> U Exp(q) | "(" E ")" | v
B --> "+" | "-"  | "*" |"/" | "^" | "||" | "&&" | "="
U --> "-"

"""

def parse(token_list):
    """converts expression to Reversed Polish Notation
    
    parseToRPN :: list(token) -> list(token)
    token :: argument|operator
    
    all operators are binary: OR, AND, WITHOUT
    
    OR has highest precedence, AND, WITHOUT - lower
    only two levels of precedence is allowed (hardwired in main loop)
    
    Example: vvv AND xxx OR yyy WITHOUT zzz -> vvv xxx yyy OR AND zzz WITHOUT
    """
    tokens = ReadAhead( (t for t in token_list) )
    return E(tokens)
    
def E(tokens):
    result = Exp(0, tokens)
    tokens.expect(EOF)
    return result

def Exp(p, tokens):
    result = []
    rapp = result.append
    rext = result.extend
    arg0 = P(tokens)
    rext(arg0)
    t, k = tokens.next_token()
    while k == OP and priority(t) >= p:
        tokens.consume_token()
        if associativity(t) == LEFT:
            q = priority(t) + 1
        else:
            q = priority(t)
        arg = Exp(q, tokens)
        rext(arg)
        rapp(t)
        t, k = tokens.next_token()
    return result

def P(tokens):
    result = []
    rapp = result.append
    rext = result.extend

    t, k = tokens.next_token()
    if k == LEFT_PAREN:
        tokens.consume_token()
        res = Exp(0, tokens)
        rext(res)
        tokens.expect(RIGHT_PAREN)
    elif k == KEYWORD:
        tokens.consume_token()
        rapp(t)
    else:
        raise ValueError, "Unexpected token: %r" % ((t, k),)
    return result

