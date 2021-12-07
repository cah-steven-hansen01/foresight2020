import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
def sub_plots_by_feature(df,top = 20,figsize = (20,20)):
    f, (axes) = plt.subplots(nrows = 3,ncols=2,figsize = figsize, sharey = True)
    xrotate = 90
    plant_area = df.groupby('Discovery/Plant Area')['NC Number'].nunique()\
                        .sort_values(ascending = False)[0:top]
    axes[0,0].bar(plant_area.index, plant_area.values)
    axes[0,0].set_title('Discovery/Plant Area')
    axes[0,0].tick_params(labelrotation = xrotate)

    product_df = df.groupby('Product')['NC Number'].nunique()\
                        .sort_values(ascending = False)[0:top]
    axes[0,1].bar(product_df.index, product_df.values)
    axes[0,1].set_title('Product Code')
    axes[0,1].tick_params(labelrotation = xrotate)

    ifm_df = df.groupby('Initial Failure Mode')['NC Number'].nunique()\
                        .sort_values(ascending = False)[0:top]
    axes[1,0].bar(ifm_df.index, ifm_df.values)
    axes[1,0].set_title('Initial Failure Mode')
    axes[1,0].tick_params(labelrotation = xrotate)

    rc_df = df.groupby('Root Cause')['NC Number'].nunique()\
                        .sort_values(ascending = False)[0:top]
    axes[1,1].bar(rc_df.index, rc_df.values)
    axes[1,1].set_title('Root Cause')
    axes[1,1].tick_params(labelrotation = xrotate)

    month_df = df.groupby('Month')['NC Number'].nunique()\
                        .sort_values(ascending = False)[0:top]
    axes[2,0].bar(month_df.index,month_df.values)
    axes[2,0].set_title('Month')
    axes[2,0].tick_params(labelrotation= xrotate)

    year_df = df.groupby('Year')['NC Number'].nunique()\
                        .sort_values(ascending = False)[0:top]
    axes[2,1].bar(year_df.index,year_df.values)
    axes[2,1].set_title('Year')
    axes[2,1].tick_params(labelrotation= xrotate)

    f.suptitle('NC Lots by Feature'.format(top),fontsize = 'x-large')
    f.tight_layout(pad = 3)

    
    
def nc_sub_plotter(df,feature, ind_feature, top = None,figsize = (20,20)):
    mask = df[feature] == ind_feature
    df = df[mask]
    features = ['Product','Initial Failure Mode','Discovery/Plant Area','Root Cause','IFM Process']
    features.remove(feature)
    f, (axes) = plt.subplots(ncols = 2, nrows = 2, figsize = figsize,sharey = True)
    xrotate = 90
    temp_plot = df.groupby(df[features[0]])['NC Number'].count().sort_values(ascending = False)[0:top]
    axes[0,0].bar(temp_plot.index, temp_plot.values)
    axes[0,0].set_title('{}'.format(features[0]))
    axes[0,0].tick_params(labelrotation = xrotate)
    temp_plot_1 = df.groupby(df[features[1]])['NC Number'].count().sort_values(ascending = False)[0:top]
    axes[0,1].bar(temp_plot_1.index, temp_plot_1.values)
    axes[0,1].set_title('{}'.format(features[1]))
    axes[0,1].tick_params(labelrotation = xrotate)
    temp_plot_2 = df.groupby(df[features[2]])['NC Number'].count().sort_values(ascending = False)[0:top]
    axes[1,0].bar(temp_plot_2.index, temp_plot_2.values)
    axes[1,0].set_title('{}'.format(features[2]))
    axes[1,0].tick_params(labelrotation = xrotate)
    temp_plot_3 = df.groupby(df[features[3]])['NC Number'].count().sort_values(ascending = False)[0:top]
    axes[1,1].bar(temp_plot_3.index, temp_plot_3.values)
    axes[1,1].set_title('{}'.format(features[3]))
    axes[1,1].tick_params(labelrotation = xrotate)
    #f.tight_layout(pad = 3)
    f.suptitle('{}: {}'.format(feature, ind_feature))

def heatmaps(df,feature_1,feature_2,figsize=(30,20) ):
    plt.figure(figsize = figsize)
    cross_df = pd.crosstab(df[feature_1],df[feature_2])
    sns.heatmap(cross_df,linewidths=.1,linecolor='black',cmap="hot_r")
    plt.title('{} by {}'.format(feature_1,feature_2))