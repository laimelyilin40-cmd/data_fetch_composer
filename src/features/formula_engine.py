"""
向量化公式引擎（給「表格拼貼器」用）

目標：
- 讓使用者用接近試算表的公式在「已 timestamp 對齊好的寬表」上加新欄位
- 公式必須是安全的（禁止任意 Python 執行）
- 計算必須是向量化（Polars Expr / LazyFrame）
"""

from __future__ import annotations

import ast
import keyword
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union, Set

import polars as pl


class FormulaError(Exception):
    pass


def _flatten_expr_args(args: Sequence[Any]) -> List[pl.Expr]:
    out: List[pl.Expr] = []
    for a in args:
        if a is None:
            continue
        if isinstance(a, list):
            out.extend([x for x in a if isinstance(x, pl.Expr)])
        elif isinstance(a, pl.Expr):
            out.append(a)
    return out


def _as_expr_list(*args: Any) -> List[pl.Expr]:
    """
    允許輸入：
    - 多個 Expr
    - 或單一 list[Expr]（例如 COLS(...) 回傳）
    - 或混合
    """
    return _flatten_expr_args(args)


def _list_from_exprs(exprs: Sequence[pl.Expr]) -> pl.Expr:
    # row-wise 運算（同 timestamp 多欄）用 list namespace（polars 1.36 沒 std_horizontal）
    return pl.concat_list(list(exprs))


def _extract_deps(formula: str, candidate_names: Set[str]) -> Set[str]:
    """
    從公式裡找出「依賴了哪些其他新欄位」。
    只關心 candidate_names（也就是 specs 的 name 集合），避免把 ROW_STD/LOGRET 這種函數名誤判為依賴。

    支援：
    - 直接引用：ret1__BTC
    - COL('ret1__BTC')
    """
    deps: Set[str] = set()
    f = (formula or "").strip()
    if not f:
        return deps
    try:
        node = ast.parse(f, mode="eval")
    except Exception:
        return deps

    for n in ast.walk(node):
        if isinstance(n, ast.Name):
            if n.id in candidate_names:
                deps.add(n.id)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == "COL":
            if n.args and isinstance(n.args[0], ast.Constant) and isinstance(n.args[0].value, str):
                colname = n.args[0].value
                if colname in candidate_names:
                    deps.add(colname)
    return deps


def _is_safe_ast(node: ast.AST) -> bool:
    """
    嚴格限制可用語法（只允許：常數、名稱、算術、括號、函數呼叫）
    """
    allowed = (
        ast.Expression,
        ast.BinOp,
        ast.UnaryOp,
        ast.Call,
        ast.Name,
        ast.Load,
        ast.Constant,
        ast.Add,
        ast.Sub,
        ast.Mult,
        ast.Div,
        ast.Pow,
        ast.Mod,
        ast.USub,
        ast.UAdd,
        ast.Compare,
        ast.Eq,
        ast.NotEq,
        ast.Gt,
        ast.GtE,
        ast.Lt,
        ast.LtE,
        ast.BoolOp,
        ast.And,
        ast.Or,
        ast.Not,
        ast.UnaryOp,
        ast.IfExp,
        ast.List,
        ast.Tuple,
    )

    for n in ast.walk(node):
        if not isinstance(n, allowed):
            return False
        # 禁止屬性/下標等危險語法
        if isinstance(n, (ast.Attribute, ast.Subscript, ast.Lambda, ast.Dict, ast.Await, ast.Yield, ast.YieldFrom)):
            return False
    return True


def _as_int(x: Any, name: str) -> int:
    try:
        return int(x)
    except Exception:
        raise FormulaError(f"{name} 必須是整數，收到：{x!r}")


def _cols_regex(all_cols: Sequence[str], pattern: str) -> List[pl.Expr]:
    try:
        reg = re.compile(pattern)
    except re.error as e:
        raise FormulaError(f"COLS(pattern) regex 無效：{e}")
    matched = [c for c in all_cols if reg.search(c)]
    return [pl.col(c) for c in matched]


@dataclass
class ColumnSpec:
    name: str
    formula: str


class FormulaEngine:
    """
    把 formula 字串轉成 Polars Expr，並可套用到 LazyFrame。
    """

    def __init__(self, all_columns: Sequence[str]):
        self.all_columns = list(all_columns)
        self._env = self._build_env()

    def _build_env(self) -> Dict[str, Any]:
        # functions (全部回傳 pl.Expr 或 list[pl.Expr])
        def COL(name: str) -> pl.Expr:
            return pl.col(str(name))

        def COLS(pattern: str) -> List[pl.Expr]:
            return _cols_regex(self.all_columns, str(pattern))

        def ABS(x: pl.Expr) -> pl.Expr:
            return x.abs()

        def LOG(x: pl.Expr) -> pl.Expr:
            # 相容不同 polars 版本：避免使用 pl.log（有些版本不存在）
            return x.log()

        def EXP(x: pl.Expr) -> pl.Expr:
            # 相容不同 polars 版本：避免使用 pl.exp（有些版本不存在）
            return x.exp()

        def SQRT(x: pl.Expr) -> pl.Expr:
            return x.sqrt()

        def SIGN(x: pl.Expr) -> pl.Expr:
            return pl.when(x > 0).then(1).when(x < 0).then(-1).otherwise(0)

        def ROUND(x: pl.Expr, digits: Any = 0) -> pl.Expr:
            return x.round(_as_int(digits, "digits"))

        def FLOOR(x: pl.Expr) -> pl.Expr:
            return x.floor()

        def CEIL(x: pl.Expr) -> pl.Expr:
            return x.ceil()

        def CLIP(x: pl.Expr, low: Any, high: Any) -> pl.Expr:
            return x.clip(low, high)

        def CLAMP(x: pl.Expr, low: Any, high: Any) -> pl.Expr:
            return x.clip(low, high)

        def LAG(x: pl.Expr, n: Any = 1) -> pl.Expr:
            return x.shift(_as_int(n, "n"))

        def DIFF(x: pl.Expr, n: Any = 1) -> pl.Expr:
            n_i = _as_int(n, "n")
            return x - x.shift(n_i)

        def PCT_CHANGE(x: pl.Expr, n: Any = 1) -> pl.Expr:
            n_i = _as_int(n, "n")
            return x / x.shift(n_i) - 1

        def LOGRET(x: pl.Expr, n: Any = 1) -> pl.Expr:
            n_i = _as_int(n, "n")
            return x.log() - x.shift(n_i).log()

        def CUMSUM(x: pl.Expr) -> pl.Expr:
            return x.cum_sum()

        def CUMPROD(x: pl.Expr) -> pl.Expr:
            return x.cum_prod()

        def ROLL_MEAN(x: pl.Expr, w: Any) -> pl.Expr:
            w_i = _as_int(w, "w")
            return x.rolling_mean(window_size=w_i, min_periods=w_i)

        def ROLL_STD(x: pl.Expr, w: Any) -> pl.Expr:
            w_i = _as_int(w, "w")
            return x.rolling_std(window_size=w_i, min_periods=w_i)

        def ROLL_SUM(x: pl.Expr, w: Any) -> pl.Expr:
            w_i = _as_int(w, "w")
            return x.rolling_sum(window_size=w_i, min_periods=w_i)

        def ROLL_ZSCORE(x: pl.Expr, w: Any) -> pl.Expr:
            w_i = _as_int(w, "w")
            mu = x.rolling_mean(window_size=w_i, min_periods=w_i)
            sd = x.rolling_std(window_size=w_i, min_periods=w_i)
            return (x - mu) / (sd + 1e-12)

        def EMA(x: pl.Expr, span: Any) -> pl.Expr:
            s_i = _as_int(span, "span")
            # polars 版本差異：ewm_mean 可能支援 span 或 alpha；這裡先用 span
            try:
                return x.ewm_mean(span=s_i, adjust=False)
            except TypeError:
                # fallback
                return x.ewm_mean(span=s_i)

        def ZSCORE(x: pl.Expr) -> pl.Expr:
            # 注意：這是全樣本 zscore（有 lookahead），若要避免請用 fit 區間版（後續可加）
            mu = x.mean()
            sd = x.std()
            return (x - mu) / (sd + 1e-12)

        def MINMAX(x: pl.Expr) -> pl.Expr:
            mn = x.min()
            mx = x.max()
            return (x - mn) / (mx - mn + 1e-12)

        def WINSORIZE(x: pl.Expr, p_low: Any = 0.01, p_high: Any = 0.99) -> pl.Expr:
            plow = float(p_low)
            phigh = float(p_high)
            if not (0.0 <= plow <= 1.0 and 0.0 <= phigh <= 1.0 and plow <= phigh):
                raise FormulaError(f"WINSORIZE 的 p_low/p_high 必須在 [0,1] 且 p_low<=p_high，收到：{p_low!r}, {p_high!r}")
            lo = x.quantile(plow)
            hi = x.quantile(phigh)
            return x.clip(lo, hi)

        def ROBUST_Z(x: pl.Expr) -> pl.Expr:
            med = x.median()
            mad = (x - med).abs().median()
            return (x - med) / (mad * 1.4826 + 1e-12)

        def RANK_NORM(x: pl.Expr) -> pl.Expr:
            r = x.rank(method="average")
            n = pl.len()
            return (r - 1) / (n - 1 + 1e-12)

        def FILL_FFILL(x: pl.Expr) -> pl.Expr:
            return x.fill_null(strategy="forward")

        def FILL_BFILL(x: pl.Expr) -> pl.Expr:
            return x.fill_null(strategy="backward")

        def FILL_ZERO(x: pl.Expr) -> pl.Expr:
            return x.fill_null(0)

        def ISNA(x: pl.Expr) -> pl.Expr:
            return x.is_null()

        def ISFINITE(x: pl.Expr) -> pl.Expr:
            try:
                return x.is_finite()
            except Exception:
                return x.is_not_null()

        def COALESCE(a: pl.Expr, b: pl.Expr) -> pl.Expr:
            return pl.coalesce([a, b])

        def IF(cond: pl.Expr, a: Any, b: Any) -> pl.Expr:
            return pl.when(cond).then(a).otherwise(b)

        def MIN(a: pl.Expr, b: pl.Expr) -> pl.Expr:
            return pl.min_horizontal(a, b)

        def MAX(a: pl.Expr, b: pl.Expr) -> pl.Expr:
            return pl.max_horizontal(a, b)

        def SET(*args: Any) -> List[pl.Expr]:
            return _as_expr_list(*args)

        def ROW_SUM(*args: Any) -> pl.Expr:
            exprs = _flatten_expr_args(args)
            return pl.sum_horizontal(*exprs) if exprs else pl.lit(None)

        def ROW_MEAN(*args: Any) -> pl.Expr:
            exprs = _flatten_expr_args(args)
            return pl.mean_horizontal(*exprs) if exprs else pl.lit(None)

        def ROW_MIN(*args: Any) -> pl.Expr:
            exprs = _flatten_expr_args(args)
            return pl.min_horizontal(*exprs) if exprs else pl.lit(None)

        def ROW_MAX(*args: Any) -> pl.Expr:
            exprs = _flatten_expr_args(args)
            return pl.max_horizontal(*exprs) if exprs else pl.lit(None)

        def ROW_COUNT_VALID(*args: Any) -> pl.Expr:
            exprs = _as_expr_list(*args)
            if not exprs:
                return pl.lit(0)
            lst = _list_from_exprs(exprs).list.drop_nulls()
            return lst.list.len()

        def ROW_STD(*args: Any) -> pl.Expr:
            exprs = _as_expr_list(*args)
            if not exprs:
                return pl.lit(None)
            lst = _list_from_exprs(exprs).list.drop_nulls()
            return lst.list.std()

        def ROW_VAR(*args: Any) -> pl.Expr:
            exprs = _as_expr_list(*args)
            if not exprs:
                return pl.lit(None)
            lst = _list_from_exprs(exprs).list.drop_nulls()
            return lst.list.var()

        def ROW_MEDIAN(*args: Any) -> pl.Expr:
            exprs = _as_expr_list(*args)
            if not exprs:
                return pl.lit(None)
            lst = _list_from_exprs(exprs).list.drop_nulls()
            return lst.list.median()

        def ROW_QUANTILE(*args: Any) -> pl.Expr:
            if len(args) < 2:
                raise FormulaError("ROW_QUANTILE(set, q) 需要兩個參數")
            *set_args, q = args
            exprs = _as_expr_list(*set_args)
            qf = float(q)
            if not (0.0 <= qf <= 1.0):
                raise FormulaError(f"ROW_QUANTILE 的 q 必須在 [0,1]，收到：{q!r}")
            if not exprs:
                return pl.lit(None)
            lst = _list_from_exprs(exprs).list.drop_nulls().list.sort()
            n = lst.list.len()
            idx = ((n - 1).cast(pl.Float64) * qf).floor().cast(pl.Int64)
            return pl.when(n <= 0).then(pl.lit(None)).otherwise(lst.list.get(idx))

        def ROW_TOPK_MEAN(*args: Any) -> pl.Expr:
            if len(args) < 2:
                raise FormulaError("ROW_TOPK_MEAN(set, k) 需要兩個參數")
            *set_args, k = args
            exprs = _as_expr_list(*set_args)
            k_i = _as_int(k, "k")
            if not exprs:
                return pl.lit(None)
            lst = _list_from_exprs(exprs).list.drop_nulls().list.sort().list.tail(k_i)
            return lst.list.mean()

        def ROW_BOTTOMK_MEAN(*args: Any) -> pl.Expr:
            if len(args) < 2:
                raise FormulaError("ROW_BOTTOMK_MEAN(set, k) 需要兩個參數")
            *set_args, k = args
            exprs = _as_expr_list(*set_args)
            k_i = _as_int(k, "k")
            if not exprs:
                return pl.lit(None)
            lst = _list_from_exprs(exprs).list.drop_nulls().list.sort().list.head(k_i)
            return lst.list.mean()

        def XS_DEMEAN(x: pl.Expr, set_: Any) -> pl.Expr:
            exprs = _as_expr_list(set_)
            if not exprs:
                return pl.lit(None)
            mu = _list_from_exprs(exprs).list.drop_nulls().list.mean()
            return x - mu

        def XS_ZSCORE(x: pl.Expr, set_: Any) -> pl.Expr:
            exprs = _as_expr_list(set_)
            if not exprs:
                return pl.lit(None)
            lst = _list_from_exprs(exprs).list.drop_nulls()
            mu = lst.list.mean()
            sd = lst.list.std()
            return (x - mu) / (sd + 1e-12)

        def XS_RANK(x: pl.Expr, set_: Any) -> pl.Expr:
            exprs = _as_expr_list(set_)
            if not exprs:
                return pl.lit(None)
            valid = [e.is_not_null() for e in exprs]
            n_valid = pl.sum_horizontal(*[v.cast(pl.Int64) for v in valid])
            lt = pl.sum_horizontal(*[(e < x).cast(pl.Int64) for e in exprs])
            eq = pl.sum_horizontal(*[(e == x).cast(pl.Int64) for e in exprs])
            rank_avg = lt.cast(pl.Float64) + (eq.cast(pl.Float64) + 1.0) / 2.0
            return pl.when(n_valid <= 0).then(pl.lit(None)).otherwise(rank_avg)

        def XS_PCTRANK(x: pl.Expr, set_: Any) -> pl.Expr:
            exprs = _as_expr_list(set_)
            if not exprs:
                return pl.lit(None)
            valid = [e.is_not_null() for e in exprs]
            n_valid = pl.sum_horizontal(*[v.cast(pl.Int64) for v in valid])
            lt = pl.sum_horizontal(*[(e < x).cast(pl.Int64) for e in exprs])
            eq = pl.sum_horizontal(*[(e == x).cast(pl.Int64) for e in exprs])
            rank_avg = lt.cast(pl.Float64) + (eq.cast(pl.Float64) + 1.0) / 2.0
            denom = (n_valid - 1).cast(pl.Float64) + 1e-12
            return pl.when(n_valid <= 1).then(pl.lit(0.0)).otherwise((rank_avg - 1.0) / denom)

        def SOFTMAX_WEIGHT(x: pl.Expr, set_: Any, temp: Any = 1.0) -> pl.Expr:
            exprs = _as_expr_list(set_)
            t = float(temp)
            if t <= 0:
                raise FormulaError(f"SOFTMAX_WEIGHT 的 temp 必須 > 0，收到：{temp!r}")
            num = (x / t).exp()
            denom = pl.sum_horizontal(*[((e / t).exp()) for e in exprs]) + 1e-12
            return num / denom

        env: Dict[str, Any] = {
            "COL": COL,
            "COLS": COLS,
            "SET": SET,
            "ABS": ABS,
            "LOG": LOG,
            "EXP": EXP,
            "SQRT": SQRT,
            "SIGN": SIGN,
            "ROUND": ROUND,
            "FLOOR": FLOOR,
            "CEIL": CEIL,
            "CLIP": CLIP,
            "CLAMP": CLAMP,
            "LAG": LAG,
            "DIFF": DIFF,
            "PCT_CHANGE": PCT_CHANGE,
            "LOGRET": LOGRET,
            "CUMSUM": CUMSUM,
            "CUMPROD": CUMPROD,
            "ROLL_MEAN": ROLL_MEAN,
            "ROLL_STD": ROLL_STD,
            "ROLL_SUM": ROLL_SUM,
            "ROLL_ZSCORE": ROLL_ZSCORE,
            "EMA": EMA,
            "ZSCORE": ZSCORE,
            "MINMAX": MINMAX,
            "WINSORIZE": WINSORIZE,
            "ROBUST_Z": ROBUST_Z,
            "RANK_NORM": RANK_NORM,
            "FILL_FFILL": FILL_FFILL,
            "FILL_BFILL": FILL_BFILL,
            "FILL_ZERO": FILL_ZERO,
            "ISNA": ISNA,
            "ISFINITE": ISFINITE,
            "COALESCE": COALESCE,
            "IF": IF,
            "MIN": MIN,
            "MAX": MAX,
            "ROW_SUM": ROW_SUM,
            "ROW_MEAN": ROW_MEAN,
            "ROW_MIN": ROW_MIN,
            "ROW_MAX": ROW_MAX,
            "ROW_COUNT_VALID": ROW_COUNT_VALID,
            "ROW_STD": ROW_STD,
            "ROW_VAR": ROW_VAR,
            "ROW_MEDIAN": ROW_MEDIAN,
            "ROW_QUANTILE": ROW_QUANTILE,
            "ROW_TOPK_MEAN": ROW_TOPK_MEAN,
            "ROW_BOTTOMK_MEAN": ROW_BOTTOMK_MEAN,
            "XS_DEMEAN": XS_DEMEAN,
            "XS_ZSCORE": XS_ZSCORE,
            "XS_RANK": XS_RANK,
            "XS_PCTRANK": XS_PCTRANK,
            "SOFTMAX_WEIGHT": SOFTMAX_WEIGHT,
        }

        # columns as variables (only if identifier-safe)
        for c in self.all_columns:
            if c.isidentifier() and not keyword.iskeyword(c):
                env[c] = pl.col(c)
        return env

    def compile_expr(self, formula: str) -> pl.Expr:
        f = (formula or "").strip()
        if not f:
            raise FormulaError("公式是空的")

        try:
            node = ast.parse(f, mode="eval")
        except SyntaxError as e:
            raise FormulaError(f"公式語法錯誤：{e.msg}（line {e.lineno} col {e.offset}）")

        if not _is_safe_ast(node):
            raise FormulaError("公式包含不允許的語法（只允許：常數/名稱/算術/比較/邏輯/函數呼叫）")

        try:
            code = compile(node, "<formula>", "eval")
            out = eval(code, {"__builtins__": {}}, dict(self._env))
        except FormulaError:
            raise
        except Exception as e:
            raise FormulaError(f"公式計算失敗：{e}")

        if isinstance(out, pl.Expr):
            return out
        # 允許 list[Expr] 的情境（例如使用者直接寫 COLS(...)）
        raise FormulaError(f"公式必須回傳一個序列（Expr），收到：{type(out).__name__}")

    def apply_specs(self, lf: pl.LazyFrame, specs: Sequence[ColumnSpec]) -> Tuple[pl.LazyFrame, List[Dict[str, Any]]]:
        """
        回傳：
        - 新的 LazyFrame（含新欄位）
        - errors：每個 spec 的錯誤（若無錯誤則空）
        """
        errors: List[Dict[str, Any]] = []
        out = lf

        # ---- 自動分階段（依賴解析 + 拓樸排序）----
        # 讓使用者「一鍵完成」，就算 specs 沒照順序排也能先算一階，再算二階。
        spec_list = list(specs)
        name_set: Set[str] = {((s.name or "").strip()) for s in spec_list if (s.name or "").strip()}
        name_set.discard("")

        deps_map: Dict[str, Set[str]] = {}
        for s in spec_list:
            nm = (s.name or "").strip()
            if not nm:
                continue
            d = _extract_deps(s.formula, name_set)
            d.discard(nm)  # 自己依賴自己視為 cycle（後面會抓）
            deps_map[nm] = d

        # Kahn topo sort（保留原始順序穩定性）
        incoming = {n: set(deps_map.get(n, set())) for n in name_set}
        ready = [((s.name or "").strip()) for s in spec_list if ((s.name or "").strip()) in name_set and not incoming.get(((s.name or "").strip()), set())]
        seen_ready = set()
        ready = [n for n in ready if not (n in seen_ready or seen_ready.add(n))]

        order: List[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            # remove edges n -> m
            for m in list(incoming.keys()):
                if n in incoming[m]:
                    incoming[m].remove(n)
                    if not incoming[m]:
                        if m not in order and m not in ready:
                            ready.append(m)

        if len(order) != len(name_set):
            # cycle or unresolved deps (usually self/cycle)
            cyclic = [n for n, inc in incoming.items() if inc]
            errors.append({"name": "(dependency)", "formula": "", "error": f"公式之間有循環依賴或順序無法解析：{cyclic[:10]}..."})
            # fallback to original order
            ordered_specs = spec_list
        else:
            spec_by_name = {((s.name or "").strip()): s for s in spec_list if (s.name or "").strip()}
            ordered_specs = [spec_by_name[n] for n in order if n in spec_by_name] + [s for s in spec_list if ((s.name or "").strip()) not in spec_by_name]

        for s in ordered_specs:
            name = (s.name or "").strip()
            if not name:
                errors.append({"name": s.name, "formula": s.formula, "error": "欄位名稱不可為空"})
                continue
            if name in out.collect_schema().names():
                errors.append({"name": name, "formula": s.formula, "error": "欄位名稱已存在（請換名）"})
                continue
            try:
                expr = self.compile_expr(s.formula).alias(name)
                out = out.with_columns([expr])
                # 允許後續公式引用剛建立的新欄位（像試算表一樣堆疊）
                if name.isidentifier() and not keyword.iskeyword(name):
                    self._env[name] = pl.col(name)
                # 允許 COLS("regex") 在後續公式選到新欄位
                # （Table Paster 常用 COLS("^RET_") 這類去選前面剛產生的欄）
                if name not in self.all_columns:
                    self.all_columns.append(name)
            except Exception as e:
                errors.append({"name": name, "formula": s.formula, "error": str(e)})
        return out, errors


