import matplotlib.pyplot as plt

def get_return_series(selected_return_df):
    rtn_series = selected_return_df.mean(axis=1)
    rtn_series.loc[2005] = 0
    # 주의: 영상속의 데이터와는 달리, 새로 업로드 된 데이터는 2006부터 존재하므로
    # 2004가 아니라 2005를 0으로 설정한 점에 주의해주세요
    rtn_series = rtn_series.sort_index()

    cum_rtn_series = (rtn_series + 1).cumprod().dropna()
    return rtn_series, cum_rtn_series


def plot_return(cum_rtn_series, rtn_series):
    fig, axes = plt.subplots(nrows=2, figsize=(15, 6), sharex=True)
    axes[0].plot(cum_rtn_series.index, cum_rtn_series, marker='o');
    axes[1].bar(rtn_series.index, rtn_series);
    axes[0].set_title("Cum return(line)");
    axes[1].set_title("Yearly return(bar)");
    plt.show()