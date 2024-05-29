from src.engine.export_engine import ExportEng
from src.engine.rules_engine import RulesEng, FunctionResult
from src.engine.filters_engine import FiltersEngine
 
import pandas as pd
import os

pd.options.mode.chained_assignment = None  # default='warn'

"""
TESTE COM 1.000.000 DE REGISTROS
REGRAS:
 - MESMO ESTUDANTE:          ALUNOS MESMA CLASSE COM NOTA MENOR QUE 6;
 - ALUNO NA TOLERANCIA:      ALUNOS MESMA CLASSE COM COM NOTAS MAIORES QUE 6;
 - ALUNO FORA DA TOLERANCIA: ALUNOS COM OS MESMOS NOMES E IDADES E CLASSES;
"""

class ExampleRulesProcess(ExportEng):
    def execute_filter(columns_to_analise):
        def for_each_group(func):
            def _for_each_group(self, df: pd.DataFrame):
                values_to_compare: dict
                df_values = FiltersEngine().get_values_column_equal(df=df, columns=columns_to_analise)

                values_to_compare: dict = df_values[columns_to_analise].T.to_dict()
                group_df_duplicates: pd.DataFrame = df.groupby(columns_to_analise)

                df_without_founds = df.copy(deep=True)
                df_founds = pd.DataFrame(columns=self.df.columns)

                for group_value in values_to_compare.values():
                    value_to_search = tuple(group_value.values())
                    
                    if len(value_to_search) == 1:
                        value_to_search = value_to_search[0]
                    result = group_df_duplicates.get_group(value_to_search)
                    
                    
                    if len(result) > 1:
                        df_founds, df_without_founds = func(result, df_founds, df_without_founds)
                        
                    
                return FunctionResult(result=df_founds, spare=df_without_founds)
            return _for_each_group
        return for_each_group

    @execute_filter(columns_to_analise=['name', 'age', 'classe'])
    def same_student(result: pd.DataFrame, df_founds: pd.DataFrame, df_without_founds: pd.DataFrame) -> pd.DataFrame:
        result.loc[:, 'ID Duplicated'] = ', '.join([str(x) for x in result['id'].values])
        df_founds = pd.concat([df_founds, result], ignore_index=True)
        df_without_founds = df_without_founds.drop(result.index)

        return df_founds, df_without_founds

    @execute_filter(columns_to_analise=['classe'])
    def note_torelance(result: pd.DataFrame, df_founds: pd.DataFrame, df_without_founds: pd.DataFrame) -> pd.DataFrame:
        result = result[result['note'] >= 6]
        df_founds = pd.concat([df_founds, result], ignore_index=True)
        df_without_founds = df_without_founds.drop(result.index)
        
        return df_founds, df_without_founds

    @execute_filter(columns_to_analise=['classe'])
    def note_out_of_torelance(result: pd.DataFrame, df_founds: pd.DataFrame, df_without_founds: pd.DataFrame) -> pd.DataFrame:
        df_founds = pd.concat([df_founds, result], ignore_index=True)
        df_without_founds = df_without_founds.drop(result.index)

        return df_founds, df_without_founds

    def set_content_rules(self) -> RulesEng:
        rules = RulesEng(df=self.df)

        rules.add_rule(
            name="Same Student",
            function=self.same_student,
            get=False,
            export=True
        )
        rules.add_rule(
            name="Note in Tolerance",
            function=self.note_torelance,
            get='Same Student',
            get_result=False,
            export=True
        )
        rules.add_rule(
            name="Note out Of Tolerance",
            function=self.note_out_of_torelance,
            get="Note in Tolerance",
            get_result=False,
            export=True
        )
        return rules


if __name__ == "__main__":
    print("reading example file")
    df = pd.read_excel("./example_data.xlsx")
    duplication_rules = ExampleRulesProcess(df=df)
    duplication_rules.execute_rules()
    print("executing rules")
    rules = duplication_rules.rules.export_rules_result()
    print("saving results")
    duplication_rules.save_result("example_result.xlsx")
