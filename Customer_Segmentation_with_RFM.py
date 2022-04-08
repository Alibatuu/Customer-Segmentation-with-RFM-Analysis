import datetime as dt
import pandas as pd
###############################################################
#GOREV 1 : Veriyi Anlama ve Hazırlama
###############################################################
# Adım 1
df_ = pd.read_csv("FLO/flo_data_20K.csv")
df = df_.copy()
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
# Adım 2
df.head(10) # İlk 10 gözlem
df.columns # Değişken isimleri
df.describe().T # Betimsel istatislik
df.isnull().sum() # Boş değerler
df.dtypes # Değişken tiplerinin incelenmesi

# Adım 3
df["order_num_total_ever"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["customer_value_total_ever"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]
# Adım 4
df["first_order_date"] = pd.to_datetime(df["first_order_date"])
df["last_order_date"] = pd.to_datetime(df["last_order_date"])
df["last_order_date_online"] = pd.to_datetime(df["last_order_date_online"])
df["last_order_date_offline"] = pd.to_datetime(df["last_order_date_offline"])
df.dtypes

# Adım 5
df.groupby('order_channel').agg({"master_id": lambda x: x.nunique(),
                                          "order_num_total_ever": "mean",
                                          "customer_value_total_ever": "mean"})
# Adım 6
df.sort_values(by="customer_value_total_ever",ascending = False).head(10)
# Adım 7
df.sort_values(by="order_num_total_ever",ascending = False).head(10)

# Adım 8
def preliminary(dataframe, csv=False):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.float_format', lambda x: '%.3f' % x)

    dataframe.head(10)
    dataframe.columns
    dataframe.describe().T
    dataframe.isnull().sum()
    dataframe.dtypes

    # Toplam alışveris sayısı ve harcaması
    dataframe["order_num_total_ever"] = dataframe["order_num_total_ever_online"] + dataframe["order_num_total_ever_offline"]
    dataframe["customer_value_total_ever"] = dataframe["customer_value_total_ever_online"] + dataframe["customer_value_total_ever_offline"]
    # Değişken tiplerini inceleme ve tarih ifade eden değişkenlerin tipinin date'e çevrilmesi
    dataframe["first_order_date"] = pd.to_datetime(dataframe["first_order_date"])
    dataframe["last_order_date"] = pd.to_datetime(dataframe["last_order_date"])
    dataframe["last_order_date_online"] = pd.to_datetime(dataframe["last_order_date_online"])
    dataframe["last_order_date_offline"] = pd.to_datetime(dataframe["last_order_date_offline"])
    dataframe.dtypes
    dataframe.groupby('order_channel').agg({"master_id": lambda x: x.nunique(),
                                          "order_num_total_ever": "mean",
                                          "customer_value_total_ever": "mean"})
    # En fazla kazancı getiren 10 müşteri
    dataframe.sort_values(by="customer_value_total_ever",ascending = False).head(10)
    # En fazla sipariş veren 10 müşteri
    dataframe.sort_values(by="order_num_total_ever",ascending = False).head(10)
    return dataframe

df = df_.copy()
df = preliminary(df, csv=True)
###############################################################
#GOREV 2 : RFM Metriklerinin Hesaplanması
###############################################################
# Adım 1
# Recency: Analiz yapılan tarihten müşterinin alışveriş yaptığı son tarih arasındaki farktır. Müşterinin firmadan
# en son ne zaman alışveriş yaptığını gösterir.
# Frequency: Sıklık demektir. Müşterinin firmadan toplam ne kadar alışveriş yaptığını ifade eder.
# Monetary: Müşterinin firmaya verdiği toplam ücrettir.
df["last_order_date"].max()
today_date = dt.datetime(2021, 6, 1)
# Adım 2,3
rfm = df.groupby('master_id').agg({'last_order_date': lambda date: (today_date-date.max()).days,
                                        'order_num_total_ever': lambda num: num,
                                        'customer_value_total_ever': lambda price:price})
# Adım 4
rfm.columns = ['recency', 'frequency', "monetary"]

###############################################################
#GOREV 3 : RF Skorunun Hesaplanması
###############################################################
# Adım 1,2
rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])

# Adım 3
rfm["RFM_SCORE"] = (rfm['recency_score'].astype(str) +
                    rfm['frequency_score'].astype(str))
###############################################################
#GOREV 4 : RF Skorunun Segment Olarak Tanımlanması
###############################################################
# Adım 1,2
seg_map = {
        r'[1-2][1-2]': 'hibernating',
        r'[1-2][3-4]': 'at_risk',
        r'[1-2]5': 'cant_loose',
        r'3[1-2]': 'about_to_sleep',
        r'33': 'need_attention',
        r'[3-4][4-5]': 'loyal_customers',
        r'41': 'promising',
        r'51': 'new_customers',
        r'[4-5][2-3]': 'potential_loyalists',
        r'5[4-5]': 'champions'
    }
rfm['segment'] = rfm['RFM_SCORE'].replace(seg_map, regex=True)
rfm = rfm[["recency", "frequency", "monetary", "segment"]]
###############################################################
#GOREV 5
###############################################################
# Adım 1
rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg(["mean", "count"])

# Adım 2
# Option A
customers = rfm[(rfm['segment'] == 'champions') | (rfm['segment'] == 'loyal_customers') & (rfm['monetary'] > 250)].index
df.index = df['master_id']
option_a = df.loc[customers]
cats = ['customer_value_total_ever','interested_in_categories_12']
rfm_new = option_a.loc[(option_a['interested_in_categories_12'].str.contains('KADIN')),cats]
rfm_new = rfm_new.reset_index()
rfm_new.columns
customers_id = rfm_new['master_id']
customers_id.to_csv("customers_id.csv")

# Option B
customers_b = rfm[(rfm['segment'] == 'about_to_sleep') | (rfm['segment'] == 'cant_loose') | (rfm['segment'] == 'new_customers')].index
df.index = df['master_id']
option_b = df.loc[customers_b]
cats = ['customer_value_total_ever','interested_in_categories_12']
rfm_new_b = option_b.loc[(option_b['interested_in_categories_12'].str.contains('ERKEK')) & (option_b['interested_in_categories_12'].str.contains('COCUK')),cats]
rfm_new_b = rfm_new_b.reset_index()
rfm_new.columns
customers_id_b = rfm_new_b['master_id']
customers_id_b.to_csv("customers_id_b.csv")
