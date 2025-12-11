# 导入函数库
from jqdata import *


## 初始化函数，设定要操作的股票、基准等等
def initialize(context):
    log.info('初始函数开始运行且全局只运行一次')
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # True为开启动态复权模式，使用真实价格交易
    set_option('use_real_price', True) 
    # 设定成交量比例
    set_option('order_volume_ratio', 1)
    # 股票类交易手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(open_tax=0, close_tax=0.001, 
                             open_commission=0.0003, close_commission=0.0003,\
                             close_today_commission=0, min_commission=5), type='stock')
                             
    # 止损状态: "normal"=正常, "clearing"=清仓中
    g.stop_loss_status = "normal"                             
    # 持仓数量
    g.stocknum = 10
    g.stock_list = []
    # 交易日计时器
    g.days = 0 
    # 调仓频率
    g.refresh_rate = 5
    # 调整止损阈值
    g.max_drawdown_threshold = 0.10
    # 记录投资组合最高价值
    g.portfolio_high = 0
    # 记录需要清仓的股票列表
    g.stocks_to_sell = []
    # 运行函数
    run_daily(trade, 'every_bar')



def check_stocks(context):
    """选择市值在5%到10%分位数之间的最小10支股票"""
    # 使用前一天作为查询日期
    query_date = context.previous_date
    
    # 查询所有A股股票的市值
    q_all = query(
        valuation.code,
        valuation.market_cap
    )
    df_all = get_fundamentals(q_all, date=query_date)  # 指定查询日期
    
    if df_all is None or len(df_all) == 0:
        log.warn(f"在{query_date}获取数据失败")
        return []
    
    log.info(f"获取数据的时间: {context.current_dt}, 数据条目数: {len(df_all)}")
    
    # 去除市值NaN值（如果有）
    df_all = df_all.dropna(subset=['market_cap'])
    
    # 按市值升序排序
    df_sorted = df_all.sort_values('market_cap', ascending=True)
    
    # 计算5%和10%分位数对应的索引位置
    n = len(df_sorted)
    idx_low = int(n * 0.05)  # 5%分位数位置（向下取整）
    idx_high = int(n * 0.10)  # 10%分位数位置（向下取整）
    
    # 确保idx_high > idx_low，避免切片为空
    if idx_low >= idx_high:
        idx_high = idx_low + 1
        if idx_high > n:
            idx_high = n
    
    # 选择市值排名在5%到10%之间的股票
    selected_df = df_sorted.iloc[idx_low:idx_high]  # 切片: [idx_low, idx_high)
    
    log.info(f"市值排名区间: [{idx_low}, {idx_high}), 选中股票数: {len(selected_df)}")
    
    if len(selected_df) == 0:
        log.warn(f"在{query_date}通过排名未选中股票")
        return []
    
    buylist = list(selected_df['code'])
    
    # 过滤停牌股票和ST股票
    buylist = filter_paused_stock(buylist)
    log.info(f"过滤停牌和ST后剩余股票数: {len(buylist)}")
    
    # 返回市值最小的10支股票（如果不足10支，则返回全部）
    return buylist[:g.stocknum]
    
def filter_paused_stock(stock_list):
    """过滤停牌股票和ST股票"""
    if not stock_list:
        return []
    
    # 获取当前时间数据字典
    current_data = get_current_data()
    
    result = []
    for stock in stock_list:
        try:
            # 直接通过键访问，会按需获取该股票的数据
            stock_data = current_data[stock]
            
            # 检查是否停牌
            is_paused = stock_data.paused
            
            # 检查是否是ST或*ST股票
            is_st = stock_data.is_st
            
            if not is_paused and not is_st:
                result.append(stock)
#                log.info(f"股票 {stock} 通过检查: 不停牌且非ST")
            else:
                if is_paused:
                    log.info(f"股票 {stock} 被过滤: 停牌")
                if is_st:
                    log.info(f"股票 {stock} 被过滤: ST或*ST股票")
        except Exception as e:
            # 如果获取股票数据失败，记录日志并过滤该股票
            log.warn(f"获取股票 {stock} 的当前数据失败: {e}，过滤该股票")
    
    log.info(f"过滤后剩余股票数量: {len(result)}")
    return result
    
    
## 计算当前回撤
def calculate_drawdown(context):
    """计算从最高点的回撤"""
    # 获取当前投资组合总价值
    current_value = context.portfolio.total_value
    
    # 更新投资组合最高价值
    if current_value > g.portfolio_high:
        g.portfolio_high = current_value
    
    # 计算回撤（从最高点下跌的百分比）
    if g.portfolio_high > 0:
        drawdown = (g.portfolio_high - current_value) / g.portfolio_high
    else:
        drawdown = 0
    
    return drawdown

## 检查是否触发止损
def check_stop_loss(context):
    """检查是否达到最大回撤阈值"""      
    # 如果已经在清仓状态，不需要再次检查
    if g.stop_loss_status == "clearing":
        return False
    
    # 如果没有持仓，不需要止损
    if len(context.portfolio.positions) == 0:
        return False
    
    # 计算当前回撤
    current_drawdown = calculate_drawdown(context)
    
    # 如果回撤超过阈值，触发止损
    if current_drawdown >= g.max_drawdown_threshold:
        log.info(f"触发止损！当前回撤: {current_drawdown:.2%}, 阈值: {g.max_drawdown_threshold:.0%}")
        g.stop_loss_status = "clearing"
        return True
    
    return False

## 清仓所有股票
def clear_all_positions(context):
    """
    清仓所有持仓的股票
    返回是否清仓完成
    """
    if len(context.portfolio.positions) == 0:
        log.info("当前无持仓，清仓完成")
        return True
    
    # 获取当前时间数据字典
    current_data = get_current_data()
    
    positions = list(context.portfolio.positions.keys())
    all_cleared = True
    
    log.warn(f"开始清仓，当前持仓: {positions}")
    
    for stock in positions:
        try:
            # 通过键访问获取股票数据
            stock_data = current_data[stock]
            # 检查股票是否可交易
            if not stock_data.paused and context.portfolio.positions[stock].closeable_amount > 0:
                try:
                    # 尝试清仓
                    order_target_value(stock, 0)
                    log.info(f"下单清仓: {stock}")
                    
                    # 检查是否还有持仓
                    if context.portfolio.positions[stock].total_amount > 0:
                        all_cleared = False
                        log.info(f"股票 {stock} 仍有持仓，需要继续清仓")
                except Exception as e:
                    log.error(f"清仓 {stock} 失败: {str(e)}")
                    all_cleared = False
            else:
                # 股票停牌或无可卖数量
                if stock_data.paused:
                    log.warn(f"股票 {stock} 停牌，无法清仓")
                all_cleared = False
        except Exception as e:
            # 如果获取股票数据失败，记录错误但不标记为清仓完成
            log.error(f"获取股票 {stock} 的数据失败: {str(e)}")
            all_cleared = False
    
    return all_cleared

## 买入股票
def buy_stocks(context):
    """买入选中的股票"""
    # 选股
    g.stock_list = check_stocks(context)
    
    if not g.stock_list:
        log.warn("没有选出股票，跳过买入")
        return
    
    # 获取当前时间数据字典，用于检查是否可以买入
    current_data = get_current_data()
    
    # 计算每只股票的投资金额
    available_cash = context.portfolio.available_cash
    num_to_buy = 0
    valid_stocks = []
    
    # 检查每只股票是否可以交易
    for stock in g.stock_list:
        try:
            stock_data = current_data[stock]
            if not stock_data.paused and not stock_data.is_st:
                valid_stocks.append(stock)
                num_to_buy += 1
                if num_to_buy >= g.stocknum:
                    break
        except Exception as e:
            log.warn(f"检查股票 {stock} 失败: {str(e)}，跳过该股票")
    
    if num_to_buy == 0:
        log.warn("没有可买入的股票")
        return
    
    cash_per_stock = available_cash / num_to_buy
    
    for stock in valid_stocks:
        try:
            order_value(stock, cash_per_stock)
            log.info(f"买入: {stock}, 金额: {cash_per_stock:.2f}")
        except Exception as e:
            log.warn(f"买入{stock}失败: {str(e)}")
    
    # 更新投资组合最高价值
    g.portfolio_high = context.portfolio.total_value
    log.info(f"买入完成，更新最高价值: {g.portfolio_high:.2f}")
    
    
## 交易函数
def trade(context):
    """主交易逻辑"""
    # 记录状态
    log.info(f"状态: {g.stop_loss_status}, 持仓: {len(context.portfolio.positions)}")
    
    # 状态1: 清仓中
    if g.stop_loss_status == "clearing":
        log.info("清仓状态: 正在清仓...")
        
        # 尝试清仓
        all_cleared = clear_all_positions(context)
        
        if all_cleared:
            log.info("清仓完成，切换回正常状态")
            g.stop_loss_status = "normal"
            g.portfolio_high = 0  # 重置最高价值
        else:
            log.info("清仓未完成，继续清仓")
        
        return
    
    # 状态2: 正常状态
    elif g.stop_loss_status == "normal":
        # 如果没有持仓，买入股票
        if len(context.portfolio.positions) == 0:
            log.info("无持仓，开始买入股票")
            buy_stocks(context)
            return
        
        # 检查是否触发止损
        if check_stop_loss(context):
            log.info("触发止损，开始清仓")
            g.stop_loss_status = "clearing"
            return
