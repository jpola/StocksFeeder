import talib as ta
import pandas as pd
import numpy as np


def ma(df, n, normalize=False, col='Close'):
    _ma = pd.Series(pd.Series.rolling(df[col], n).mean(), name='MA_' + str(n))
    if normalize:
        _ma = z_score(_ma)
    return _ma


def ema(df, n, normalize=False, col='Close'):
    series = df[col].as_matrix()
#     _ema = pd.Series(pd.Series.ewm(df[col], span=n, min_periods=n - 1).mean(),name='EMA_' + str(n))
    _ema=pd.Series(ta.EMA(series,n),index=df.index, name='EMA_' + str(n))
    if normalize:
        _ema = z_score(_ema)
    return _ema


# Chaikin Oscillator for Ema_n Ema m
def cho(df, n=3, m=10, normalize=False):
    h = df['High'].as_matrix()
    l = df['Low'].as_matrix()
    c = df['Close'].as_matrix()
    v = df['Volume'].as_matrix()
    
#     ad = (2 * df['Close'] - df['High'] - df['Low']) / (df['High'] - df['Low']) * df['Volume']
#     _chaikin = pd.Series(pd.Series.ewm(ad, span=n, min_periods=n - 1).mean() -
#                          pd.Series.ewm(ad, span=m, min_periods=m - 1).mean(),
#                          name='CHAIKIN' + str(n) + '_' + str(m))
    _chaikin = pd.Series(ta.ADOSC(h,l,c,v,fastperiod=n,slowperiod=m),index=df.index,name='CHAIKIN' + str(n) + '_' + str(m))
    if normalize:
        _chaikin = z_score(_chaikin)

    return _chaikin


# Average Directional Index for period n
def adx(df, n=50, normalize=False):
    h = df['High'].as_matrix()
    l = df['Low'].as_matrix()
    c = df['Close'].as_matrix()

    _adx = pd.Series(ta.ADX(h, l, c, timeperiod=n), index=df.index, name="ADX_" + str(n))

    if normalize:
        _adx = z_score(_adx)

    return _adx


# Commodity Channel Index for period
def cci(df, n=20, normalize=False):
    h = df['High'].as_matrix()
    l = df['Low'].as_matrix()
    c = df['Close'].as_matrix()

    _cci = pd.Series(ta.CCI(h, l, c, timeperiod=n), index=df.index, name="CCI_" + str(n))

    if normalize:
        _cci = z_score(_cci)

    return _cci


# Moving average Convergence / Divergence for n-fast, m-slow, s-signalperiod
def macd(df, n=12, m=26, s=9, normalize=False, col='Close'):
    c = df[col].as_matrix()

    # (macd, macdsignal, macdhist)
    _macd = ta.MACD(c, fastperiod=n, slowperiod=m, signalperiod=s)

    _str_param = "_" + str(n) + "_" + str(m) + "_" + str(s)
    #MP:All 3 signals are the same - corrected
    _macd_ts = pd.Series(_macd[0], index=df.index, name='MACD' + _str_param)
    _macdsignal_ts = pd.Series(_macd[1], index=df.index, name='MACD_SIG' + _str_param)
    _macdhist_ts = pd.Series(_macd[2], index=df.index, name='MACD_HIST' + _str_param)

    if normalize:
        _macd_ts = z_score(_macd_ts)
        _macdsignal_ts = z_score(_macdsignal_ts)
        _macdhist_ts = z_score(_macdhist_ts)

    return _macd_ts, _macdsignal_ts, _macdhist_ts


# Momentum for period n
def mom(df, n=5, normalize=False, col='Close'):
    c = df[col].as_matrix()
    _mom = pd.Series(ta.MOM(c, timeperiod=n),
                     index=df.index, name="MOM_" + str(n))
    if normalize:
        _mom = z_score(_mom)
    return _mom


# Percentage Price Oscilator for fastperiod - n, slowperiod - m
def ppo(df, n=12, m=26, normalize=False, col='Close'):
    c = df[col].as_matrix()
    _ppo = pd.Series(ta.PPO(c, fastperiod=n, slowperiod=m,matype=1),
                     index=df.index, name="PPO_" + str(n) + "_" + str(m))

    if normalize:
        _ppo = z_score(_ppo)

    return _ppo


# Absolute Price Oscilator for fastperiod -n, slowperiod - m
def apo(df, n=5, m=10, normalize=False, col='Close'):
    c = df[col].as_matrix()
    _apo = pd.Series(ta.APO(c, fastperiod=n, slowperiod=m),
                     index=df.index, name="APO_" + str(n) + "_" + str(m))
    if normalize:
        _apo = z_score(_apo)

    return _apo


# Relative Strength Index for period n
def rsi(df, n=14, normalize=False, col='Close'):
    c = df[col].as_matrix()
    _rsi = pd.Series(ta.RSI(c, timeperiod=n), index=df.index, name="RSI_" + str(n))

    if normalize:
        _rsi = z_score(_rsi)

    return _rsi


# Daily Price Rate of Change for timeperiod n

# def roc(df, n=10, normalize=True, col='Close'):
# 
#     M = df[col].diff(n - 1) #should be n to account for 1 day diff
#     N = df[col].shift(n - 1) #should be n
# 
#     _roc = pd.Series(M / N, index=df.index, name='ROC_' + str(n))
# 
#     if normalize:
#         _roc = z_score(_roc)
#     return _roc
#changed by MP
def roc(df, n=10, normalize=False, col='Close'):
    c = df[col].as_matrix()

    _ac = pd.Series(ta.ROC(c, timeperiod=n), index=df.index,
                    name="ROC_" + str(n))

    if normalize:
        _ac = z_score(_ac)

    return _ac

# Williams Accumulation / Distribution for timeperiod n
def wad(df, n=10, normalize=False):
    
    dCl=mom(df,1,False, col='Close')
    _df=df.join(df['Close'].shift(1),rsuffix='_Lag')
    trueHL=pd.DataFrame({'trueHigh':_df[['High','Close_Lag']].max(axis=1,skipna=False),'trueLow': _df[['Low','Close_Lag']].min(axis=1,skipna=False)})
    _wad=df['Close'] - trueHL['trueLow'].where(cond=dCl>0,other=trueHL['trueHigh'])
    _wad[dCl==0]=0
    _wad=_wad.cumsum()
    
    _wad=pd.Series(_wad, index=df.index, name='WAD_' + str(n))
    
    if normalize:
        _wad = z_score(_wad)

    return _wad

def wpr(df, n=14, normalize=False):
    h = df['High'].as_matrix()
    l = df['Low'].as_matrix()
    c = df['Close'].as_matrix()

    _wpr = pd.Series(ta.WILLR(h, l, c, timeperiod=n), index=df.index, name="WPR_" + str(n))

    if normalize:
        _wpr = z_score(_wpr)

    return _wpr


def ac(df, n=5, k=5, normalize=False, col='Close'):
    c = df[col].as_matrix()

    # Calculate of momentum over k
    _mom = ta.MOM(c, timeperiod=k)
    # Acceleration is the change of momentum over n
    _ac = pd.Series(ta.MOM(_mom, timeperiod=n), index=df.index,
                    name="AC_" + str(n) + "_" + str(k))

    if normalize:
        _ac = z_score(_ac)

    return _ac

def z_score(ts):
    _mu = ts.mean()
    _std = ts.std()
    _zscore = (ts - _mu) / _std
    return _zscore


def calculate_indicators(df, normalize=False):
    _ti = []

    _ti.append(roc(df, 1, normalize))
    _ti.append(roc(df, 2, normalize))
    _ti.append(roc(df, 3, normalize))
    _ti.append(roc(df, 5, normalize))
    _ti.append(roc(df, 10, normalize))
    _ti.append(roc(df, 12, normalize))
    _ti.append(roc(df, 25, normalize))
    _ti.append(roc(df, 200, normalize))
    
    # _ti.append(ma(df, 10, normalize))
    
    _ti.append(adx(df, 7, normalize))
    _ti.append(adx(df, 14, normalize))
    _ti.append(adx(df, 50, normalize))
    _ti.append(adx(df, 200, normalize))
    _ti.append(cho(df, 3, 10, normalize))
    _ti.append(ema(df, 7, normalize))
    _ti.append(ema(df, 50, normalize))
    _ti.append(ema(df, 200, normalize))
    _ti.append(macd(df, 12, 26, 9, normalize))
    _ti.append(wad(df, 10, normalize))
    _ti.append(ac(df, 5, 5, normalize))
    _ti.append(cci(df, 20, normalize))    
    _ti.append(mom(df, 5, normalize))
    _ti.append(ppo(df, 12, 26, normalize))  # which order, which is reffered in papaer
    #_ti.append(apo(df, 5, 10, normalize))  # same as above
    _ti.append(rsi(df, 14, normalize))
    _ti.append(wpr(df, 14, normalize))




    for t in _ti:
        df = df.join(t)

    return df
