import pandas as pd

class FiltersEngine():
    def by_value(self, df:pd.DataFrame, columns:str=False, value:str|int=False, dict_values:dict=False) -> pd.DataFrame:
        """
            Return values that equals values or passing a dict_value
            
            dict_values = {
                'value_name': value
            }
        """
        if 0 == sum(1 for x in [value, dict_values] if x) > 1 :
            raise Exception('FiltersEngine > by_value: Select "dict_value" or "value""')
        
        if value:
            df_result = df[df[columns] == value]
        
        elif dict_values:
            df_result = df.loc[(df[list(dict_values)] == pd.Series(dict_values)).all(axis=1)]

        return df_result
    
    def get_values_column_equal(self, df:pd.DataFrame, columns:list) -> pd.DataFrame:
        """
            Return values from rows that have the same value in the 'columns'
        """
        df = df[df.duplicated(columns)]
        df_result = df.drop_duplicates(columns, keep='first')
        return df_result
    
    def get_column_equal(self, df:pd.DataFrame, columns:list) -> pd.DataFrame:
        """
            Return values from rows that have the same value in the 'columns'
        """
        df_result = df[df.duplicated(columns)]
        return df_result
    
    def get_value_percentage_range(self, df:pd.DataFrame, value, column:str, percentage:int|float) -> pd.DataFrame:
        """
            Return a percentage range (100% - value and 100% + value)
            - value -> 0.1 |- 0.9
        """
        df_result = df[
            (df[column] > value * 1 - percentage) & 
            (df[column] < value * 1 + percentage)
        ]
        return df_result
    
    

    


