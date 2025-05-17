import pandas as pd
import sys
import os
from PyQt6.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, 
                           QHBoxLayout, QFileDialog, QLabel, QWidget, QTabWidget, QTableView,
                           QHeaderView, QMessageBox, QLineEdit, QFormLayout, QInputDialog, QDialog)
from PyQt6.QtCore import Qt, QAbstractTableModel
from typing import Dict, List, Optional, Union, Any, Callable
import re

class PandasModel(QAbstractTableModel):
    """用于在QTableView中显示Pandas DataFrame的模型类"""
    
    def __init__(self, data: pd.DataFrame):
        super().__init__()
        self._data = data

    def rowCount(self, parent: Optional[Any] = None) -> int:
        return len(self._data)

    def columnCount(self, parent: Optional[Any] = None) -> int:
        return len(self._data.columns)

    def data(self, index: Any, role: int = Qt.ItemDataRole.DisplayRole) -> Optional[str]:
        if not index.isValid():
            return None
            
        if role == Qt.ItemDataRole.DisplayRole:
            value = self._data.iloc[index.row(), index.column()]
            # 处理不同类型的数据
            if pd.isna(value):
                return ""
            return str(value)
            
        return None

    def headerData(self, section: int, orientation: Qt.Orientation, 
                  role: int = Qt.ItemDataRole.DisplayRole) -> Optional[str]:
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._data.columns[section])
            if orientation == Qt.Orientation.Vertical:
                return str(self._data.index[section])
        return None


class ExcelProcessor:
    """Excel处理类，用于读取和处理Excel文件中的表格数据"""
    
    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        self.tables: Dict[str, pd.DataFrame] = {}
        self.processed_tables: Dict[str, pd.DataFrame] = {}
        
    def set_file_path(self, file_path: str) -> None:
        """设置Excel文件路径"""
        self.file_path = file_path
        
    def read_excel(self) -> bool:
        """读取Excel文件中的'贸易经营风险指标'表"""
        if not self.file_path:
            return False
            
        try:
            # 读取整个工作表
            print(f"正在读取文件: {self.file_path}")
            sheet_data = pd.read_excel(self.file_path, sheet_name="贸易经营风险指标", header=None)
            
            # 表格名称及其正则表达式模式
            table_patterns = [
                (r"一、\s*逾期还款业务", "一、逾期还款业务"),
                (r"二、\s*付款逾期未到货\(1\)", "二、付款逾期未到货(1)"),
                (r"三、\s*付款逾期未到货\(2、集港及在途部分\)", "三、付款逾期未到货(2、集港及在途部分)"),
                (r"四、\s*转口销售逾期未开证", "四、转口销售逾期未开证"),
                (r"五、\s*签约未到货", "五、签约未到货"),
                (r"六、\s*逾期未交货/未验收/未退质保金/未结算", "六、逾期未交货/未验收/未退质保金/未结算"),
                (r"七、\s*投标保证金逾期退还表", "七、投标保证金逾期退还表"),
                (r"八、\s*现货敞口90天及以上库存", "八、现货敞口90天及以上库存"),
                (r"九、\s*期现结合90天及以上库存", "九、期现结合90天及以上库存")
            ]
            
            # 查找每个表格的开始位置
            start_rows = []
            for pattern, name in table_patterns:
                found = False
                for i, row in sheet_data.iterrows():
                    cell_value = str(row[0]) if not pd.isna(row[0]) else ""
                    if re.search(pattern, cell_value):
                        print(f"找到表格标题: {cell_value} 在行 {i}")
                        start_rows.append((int(i)+1, name))  # 使用int()确保i是整数类型
                        found = True
                        break
                if not found:
                    print(f"警告: 未找到表格 '{name}'")
            
            # 按行号排序表格起始位置
            start_rows.sort(key=lambda x: x[0])
            
            # 计算每个表格的结束位置
            for i in range(len(start_rows)):
                start_row, table_name = start_rows[i]
                
                # 如果不是最后一个表格，则下一个表格的开始行是当前表格的结束行
                if i < len(start_rows) - 1:
                    end_row = start_rows[i + 1][0] - 1
                else:
                    # 对于最后一个表格，查找连续的空行作为结束标志
                    end_row = self._find_table_end(sheet_data, start_row)
                
                # 提取表格数据，确保至少包含两行（标题行和至少一行数据）
                if end_row - start_row < 1:
                    print(f"警告: 表格 '{table_name}' 太短，可能无效")
                    continue
                
                table_data = sheet_data.iloc[start_row:end_row+1].copy()
                
                # 处理标题行，设置为DataFrame的列名
                headers = table_data.iloc[0].fillna('')
                table_data = table_data.iloc[1:].copy()
                table_data.columns = headers
                
                # 清除空行
                table_data = table_data.dropna(how='all')
                
                # 存储表格数据
                self.tables[table_name] = table_data
                print(f"成功读取表格: {table_name}, 行数: {len(table_data)}")
                
            return True
            
        except Exception as e:
            print(f"读取Excel文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def _find_table_end(self, df: pd.DataFrame, start_row: int, empty_threshold: int = 3) -> int:
        """查找表格结束位置，通过检测连续空行"""
        empty_count = 0
        for i in range(start_row + 1, len(df)):
            # 检查行是否为空（所有单元格都是NaN或空字符串）
            row_values = df.iloc[i].fillna('').astype(str)
            if row_values.str.strip().str.len().sum() == 0 or row_values.isna().all():
                empty_count += 1
                if empty_count >= empty_threshold:
                    return i - empty_threshold
            else:
                empty_count = 0
                
        return len(df) - 1
    
    def get_tables(self) -> Dict[str, pd.DataFrame]:
        """获取所有读取的表格"""
        return self.tables
    
    def get_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """获取指定名称的表格"""
        return self.tables.get(table_name, None)
    
    def get_processed_table(self, table_name: str) -> Optional[pd.DataFrame]:
        """获取指定名称的处理后表格"""
        return self.processed_tables.get(table_name, None)
    
    def process_overdue_payment(self, threshold: float = 3000) -> bool:
        """处理'一、逾期还款业务'表"""
        table_name = "一、逾期还款业务"
        table = self.get_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 查找合同号列，有可能名称不完全一致
            contract_column = None
            possible_contract_columns = ['合同号', '合同', '合同编号', '合同序号', '编号']
            for col in possible_contract_columns:
                if col in df.columns:
                    contract_column = col
                    print(f"找到合同号列: {col}")
                    break
            
            if contract_column is None:
                print("警告: 未找到合同号列，将使用所有行")
                # 1. 清除所有合计行 - 由于没有合同号列，无法使用合同号过滤
                # 尝试查找合计行或小计行，通常这些行的第一列或者经营单位列会包含"合计"或"小计"字样
                if '经营单位' in df.columns:
                    df = df[~df['经营单位'].astype(str).str.contains('合计|小计', na=False)]
                    print("已通过'经营单位'列过滤合计行")
                elif df.columns[0] in df.columns:  # 使用第一列
                    first_col = df.columns[0]
                    df = df[~df[first_col].astype(str).str.contains('合计|小计', na=False)]
                    print(f"已通过第一列'{first_col}'过滤合计行")
            else:
                # 1. 清除所有合计行（通过清除所有合同号为空的行来实现）
                df = df[df[contract_column].notna()]
                print(f"已通过'{contract_column}'列过滤合计行，剩余行数: {len(df)}")
            
            # 检查是否有必要的列
            required_columns = ['逾期事由', '金额/万元', '板群', '经营单位', '客户', '产品']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 缺少必要的列: {missing_columns}")
                # 尝试查找替代列
                column_maps = {
                    '逾期事由': ['逾期原因', '事由', '原因'],
                    '金额/万元': ['金额', '金额(万元)', '金额（万元）', '逾期金额', '逾期金额/万元'],
                    '板群': ['分板群', '业务板群'],
                    '经营单位': ['经营单位名称', '单位', '部门'],
                    '客户': ['客户名称', '客户名', '购货单位'],
                    '产品': ['产品名称', '品名', '货物']
                }
                
                # 创建列名映射
                column_mapping = {}
                for required_col, alternatives in column_maps.items():
                    if required_col not in df.columns:
                        for alt_col in alternatives:
                            if alt_col in df.columns:
                                column_mapping[alt_col] = required_col
                                print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                                break
                
                # 重命名列
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    print(f"列重命名后的列名: {list(df.columns)}")
            
            # 检查重命名后是否还有缺失的列
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 仍然缺少必要的列: {missing_columns}")
                return False
            
            # 2. 增加两列：【控货类业务逾期金额】和【授信类业务逾期金额】
            df['控货类业务逾期金额'] = 0.0
            df['授信类业务逾期金额'] = 0.0
            
            # 应用规则
            control_cargo_conditions = (df['逾期事由'] == '控货逾期未收款') | (df['逾期事由'] == '已出运未收汇（非OA）')
            
            # 设置控货类业务逾期金额
            df.loc[control_cargo_conditions, '控货类业务逾期金额'] = df.loc[control_cargo_conditions, '金额/万元']
            
            # 设置授信类业务逾期金额
            df.loc[~control_cargo_conditions, '授信类业务逾期金额'] = df.loc[~control_cargo_conditions, '金额/万元']
            
            # 3. 检查并保留指定列
            columns_to_keep = ['板群', '经营单位', '客户', '产品', '金额/万元', '控货类业务逾期金额', '授信类业务逾期金额', 
                              '本周还款计划', '集团在手(万元)', '集团占用(万元)']
            
            # 检查并处理可能缺少的非必要列
            for col in ['本周还款计划', '集团在手(万元)', '集团占用(万元)']:
                if col not in df.columns:
                    print(f"警告: 列 '{col}' 不存在，将添加空列")
                    df[col] = ''
            
            # 保留指定列
            df_simplified = df[columns_to_keep].copy()
            
            # 4. 按照板群、经营单位、客户作为维度合并数据
            def combine_text_values(series) -> str:
                """合并文本值，去重并用'/'分隔"""
                unique_values = list(set(str(val) for val in series if not pd.isna(val) and str(val).strip() != ''))
                return '/'.join(unique_values) if unique_values else ''
            
            def sum_numeric_values(series) -> float:
                """合计数值"""
                return series.sum()
            
            # 定义每列的聚合函数
            aggregations = {
                '产品': combine_text_values,
                '金额/万元': sum_numeric_values,
                '控货类业务逾期金额': sum_numeric_values,
                '授信类业务逾期金额': sum_numeric_values,
                '本周还款计划': combine_text_values,
                '集团在手(万元)': combine_text_values,
                '集团占用(万元)': combine_text_values
            }
            
            # 执行分组聚合
            grouped_df = df_simplified.groupby(['板群', '经营单位', '客户']).agg(aggregations).reset_index()
            
            # 5. 按照金额筛选，只有【金额/万元】合计超过阈值的客户才需要列出
            # 确保金额列为数值类型
            grouped_df['金额/万元'] = pd.to_numeric(grouped_df['金额/万元'], errors='coerce')
            
            # 计算每个客户的金额合计
            customer_amounts = grouped_df.groupby('客户')['金额/万元'].sum()
            large_customers = customer_amounts[customer_amounts >= threshold].index.tolist()
            
            print(f"金额合计超过{threshold}万的客户数量: {len(large_customers)}")
            
            # 分离大客户和小客户
            large_df = grouped_df[grouped_df['客户'].isin(large_customers)]
            small_df = grouped_df[~grouped_df['客户'].isin(large_customers)]
            
            # 按照经营单位合并小客户
            if not small_df.empty:
                # 为小客户创建一个"其他"客户
                small_df['客户'] = '其他'
                
                # 将指定列的内容改为"见明细表"
                columns_to_modify = ['产品', '本周还款计划', '集团在手(万元)', '集团占用(万元)']
                for col in columns_to_modify:
                    if col in small_df.columns:
                        small_df[col] = '见明细表'
                
                # 按照板群和经营单位重新聚合
                small_df_agg = small_df.groupby(['板群', '经营单位', '客户']).agg(aggregations).reset_index()
                
                # 合并大客户和聚合后的小客户
                final_df = pd.concat([large_df, small_df_agg], axis=0).reset_index(drop=True)
            else:
                final_df = large_df.copy()
            
            # 存储处理后的表格
            self.processed_tables[table_name] = final_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(final_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False
    
    def process_overdue_delivery(self, threshold: float = 3000) -> bool:
        """处理'二、付款逾期未到货(1)'表"""
        table_name = "二、付款逾期未到货(1)"
        table = self.get_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 查找合同号列，有可能名称不完全一致
            contract_column = None
            possible_contract_columns = ['合同号', '合同', '合同编号', '合同序号', '编号']
            for col in possible_contract_columns:
                if col in df.columns:
                    contract_column = col
                    print(f"找到合同号列: {col}")
                    break
            
            if contract_column is None:
                print("警告: 未找到合同号列，将使用所有行")
                # 1. 清除所有合计行 - 由于没有合同号列，无法使用合同号过滤
                # 尝试查找合计行或小计行，通常这些行的第一列或者经营单位列会包含"合计"或"小计"字样
                if '经营单位' in df.columns:
                    df = df[~df['经营单位'].astype(str).str.contains('合计|小计', na=False)]
                    print("已通过'经营单位'列过滤合计行")
                elif df.columns[0] in df.columns:  # 使用第一列
                    first_col = df.columns[0]
                    df = df[~df[first_col].astype(str).str.contains('合计|小计', na=False)]
                    print(f"已通过第一列'{first_col}'过滤合计行")
            else:
                # 1. 清除所有合计行（通过清除所有合同号为空的行来实现）
                df = df[df[contract_column].notna()]
                print(f"已通过'{contract_column}'列过滤合计行，剩余行数: {len(df)}")
            
            # 检查是否有必要的列
            required_columns = ['逾期事由', '金额/万元', '板群', '经营单位', '供应商', '产品']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 缺少必要的列: {missing_columns}")
                # 尝试查找替代列
                column_maps = {
                    '逾期事由': ['逾期原因', '事由', '原因'],
                    '金额/万元': ['金额', '金额(万元)', '金额（万元）', '逾期金额', '逾期金额/万元'],
                    '板群': ['分板群', '业务板群'],
                    '经营单位': ['经营单位名称', '单位', '部门'],
                    '供应商': ['供应商名称', '供应商名', '供货单位'],
                    '产品': ['产品名称', '品名', '货物']
                }
                
                # 创建列名映射
                column_mapping = {}
                for required_col, alternatives in column_maps.items():
                    if required_col not in df.columns:
                        for alt_col in alternatives:
                            if alt_col in df.columns:
                                column_mapping[alt_col] = required_col
                                print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                                break
                
                # 重命名列
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    print(f"列重命名后的列名: {list(df.columns)}")
            
            # 检查重命名后是否还有缺失的列
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 仍然缺少必要的列: {missing_columns}")
                return False
            
            # 2. 增加两列：【控货类业务逾期金额】和【授信类业务逾期金额】
            df['控货类业务逾期金额'] = 0.0
            df['授信类业务逾期金额'] = 0.0
            
            # 应用规则
            control_cargo_conditions = (df['逾期事由'] == '控货逾期未收款') | (df['逾期事由'] == '已出运未收汇（非OA）')
            
            # 设置控货类业务逾期金额
            df.loc[control_cargo_conditions, '控货类业务逾期金额'] = df.loc[control_cargo_conditions, '金额/万元']
            
            # 设置授信类业务逾期金额
            df.loc[~control_cargo_conditions, '授信类业务逾期金额'] = df.loc[~control_cargo_conditions, '金额/万元']
            
            # 3. 检查并保留指定列
            columns_to_keep = ['板群', '经营单位', '供应商', '产品', '金额/万元', '控货类业务逾期金额', '授信类业务逾期金额', 
                              '本周到货计划', '集团在手(万元)', '集团占用(万元)']
            
            # 检查并处理可能缺少的非必要列
            for col in ['本周到货计划', '集团在手(万元)', '集团占用(万元)']:
                if col not in df.columns:
                    print(f"警告: 列 '{col}' 不存在，将添加空列")
                    df[col] = ''
            
            # 保留指定列
            df_simplified = df[columns_to_keep].copy()
            
            # 4. 按照板群、经营单位、供应商作为维度合并数据
            def combine_text_values(series) -> str:
                """合并文本值，去重并用'/'分隔"""
                unique_values = list(set(str(val) for val in series if not pd.isna(val) and str(val).strip() != ''))
                return '/'.join(unique_values) if unique_values else ''
            
            def sum_numeric_values(series) -> float:
                """合计数值"""
                return series.sum()
            
            # 定义每列的聚合函数
            aggregations = {
                '产品': combine_text_values,
                '金额/万元': sum_numeric_values,
                '控货类业务逾期金额': sum_numeric_values,
                '授信类业务逾期金额': sum_numeric_values,
                '本周到货计划': combine_text_values,
                '集团在手(万元)': combine_text_values,
                '集团占用(万元)': combine_text_values
            }
            
            # 执行分组聚合
            grouped_df = df_simplified.groupby(['板群', '经营单位', '供应商']).agg(aggregations).reset_index()
            
            # 5. 按照金额筛选，只有【金额/万元】合计超过阈值的供应商才需要列出
            # 确保金额列为数值类型
            grouped_df['金额/万元'] = pd.to_numeric(grouped_df['金额/万元'], errors='coerce')
            
            # 计算每个供应商的金额合计
            supplier_amounts = grouped_df.groupby('供应商')['金额/万元'].sum()
            large_suppliers = supplier_amounts[supplier_amounts >= threshold].index.tolist()
            
            print(f"金额合计超过{threshold}万的供应商数量: {len(large_suppliers)}")
            
            # 分离大供应商和小供应商
            large_df = grouped_df[grouped_df['供应商'].isin(large_suppliers)]
            small_df = grouped_df[~grouped_df['供应商'].isin(large_suppliers)]
            
            # 按照经营单位合并小供应商
            if not small_df.empty:
                # 为小供应商创建一个"其他"供应商
                small_df['供应商'] = '其他'
                
                # 将指定列的内容改为"见明细表"
                columns_to_modify = ['产品', '本周到货计划', '集团在手(万元)', '集团占用(万元)']
                for col in columns_to_modify:
                    if col in small_df.columns:
                        small_df[col] = '见明细表'
                
                # 按照板群和经营单位重新聚合
                small_df_agg = small_df.groupby(['板群', '经营单位', '供应商']).agg(aggregations).reset_index()
                
                # 合并大供应商和聚合后的小供应商
                final_df = pd.concat([large_df, small_df_agg], axis=0).reset_index(drop=True)
            else:
                final_df = large_df.copy()
            
            # 存储处理后的表格
            self.processed_tables[table_name] = final_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(final_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def process_inventory(self, threshold: float = 3000) -> bool:
        """处理'八、现货敞口90天及以上库存'表"""
        table_name = "八、现货敞口90天及以上库存"
        table = self.get_table(table_name)
        
        if table is None or len(table) == 0:
            print(f"表格 '{table_name}' 不存在或为空")
            return False
            
        try:
            # 复制表格以便处理
            df = table.copy()
            
            # 输出列名以便调试
            print(f"表格 '{table_name}' 的列名: {list(df.columns)}")
            
            # 1. 清除所有合计行（通过清除所有【产品】为空的行来实现）
            if '产品' in df.columns:
                df = df[df['产品'].notna()]
                print(f"已通过'产品'列过滤合计行，剩余行数: {len(df)}")
            else:
                print("警告: 未找到'产品'列，无法过滤合计行")
                
                # 尝试查找替代列
                possible_product_columns = ['品名', '货物', '产品名称', '商品']
                product_column = None
                for col in possible_product_columns:
                    if col in df.columns:
                        product_column = col
                        print(f"使用替代列 '{col}' 作为产品列")
                        df = df[df[product_column].notna()]
                        print(f"已通过'{product_column}'列过滤合计行，剩余行数: {len(df)}")
                        # 重命名列
                        df = df.rename(columns={product_column: '产品'})
                        break
            
            # 检查是否有必要的列
            required_columns = ['板群', '经营单位', '库存地点', '产品', '库存/万元', '本周处理计划']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 缺少必要的列: {missing_columns}")
                # 尝试查找替代列
                column_maps = {
                    '板群': ['分板群', '业务板群'],
                    '经营单位': ['经营单位名称', '单位', '部门'],
                    '库存地点': ['存放地点', '仓库', '仓储地点', '存储地点'],
                    '产品': ['品名', '货物', '产品名称', '商品'],
                    '库存/万元': ['金额/万元', '金额', '库存金额', '库存金额/万元', '库存额', '库存额/万元'],
                    '本周处理计划': ['处理计划', '本周计划', '计划']
                }
                
                # 创建列名映射
                column_mapping = {}
                for required_col, alternatives in column_maps.items():
                    if required_col not in df.columns:
                        for alt_col in alternatives:
                            if alt_col in df.columns:
                                column_mapping[alt_col] = required_col
                                print(f"使用替代列 '{alt_col}' 代替 '{required_col}'")
                                break
                
                # 重命名列
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    print(f"列重命名后的列名: {list(df.columns)}")
            
            # 检查重命名后是否还有缺失的列
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                print(f"警告: 仍然缺少必要的列: {missing_columns}")
                return False
            
            # 2. 保留指定列
            columns_to_keep = ['板群', '经营单位', '库存地点', '产品', '库存/万元', '本周处理计划']
            
            # 检查并处理可能缺少的非必要列
            for col in ['本周处理计划']:
                if col not in df.columns:
                    print(f"警告: 列 '{col}' 不存在，将添加空列")
                    df[col] = ''
            
            # 保留指定列
            df_simplified = df[columns_to_keep].copy()
            
            # 3. 按照板群、经营单位、库存地点作为维度合并数据
            def combine_text_values(series) -> str:
                """合并文本值，去重并用'/'分隔"""
                unique_values = list(set(str(val) for val in series if not pd.isna(val) and str(val).strip() != ''))
                return '/'.join(unique_values) if unique_values else ''
            
            def sum_numeric_values(series) -> float:
                """合计数值"""
                return series.sum()
            
            # 定义每列的聚合函数
            aggregations = {
                '产品': combine_text_values,
                '库存/万元': sum_numeric_values,
                '本周处理计划': combine_text_values
            }
            
            # 执行分组聚合
            grouped_df = df_simplified.groupby(['板群', '经营单位', '库存地点']).agg(aggregations).reset_index()
            
            # 确保金额列为数值类型
            grouped_df['库存/万元'] = pd.to_numeric(grouped_df['库存/万元'], errors='coerce')
            
            # 5. 按照金额筛选，只有【库存/万元】合计超过阈值的库存地点才需要列出
            # 计算每个库存地点的金额合计
            location_amounts = grouped_df.groupby('库存地点')['库存/万元'].sum()
            large_locations = location_amounts[location_amounts >= threshold].index.tolist()
            
            print(f"库存金额合计超过{threshold}万的库存地点数量: {len(large_locations)}")
            
            # 分离大金额和小金额库存地点
            large_df = grouped_df[grouped_df['库存地点'].isin(large_locations)]
            small_df = grouped_df[~grouped_df['库存地点'].isin(large_locations)]
            
            # 按照经营单位合并小金额库存地点
            if not small_df.empty:
                # 为小金额库存地点创建一个"其他"
                small_df['库存地点'] = '其他'
                
                # 将指定列的内容改为"见明细表"
                columns_to_modify = ['产品', '本周处理计划']
                for col in columns_to_modify:
                    if col in small_df.columns:
                        small_df[col] = '见明细表'
                
                # 按照板群和经营单位重新聚合
                small_df_agg = small_df.groupby(['板群', '经营单位', '库存地点']).agg(aggregations).reset_index()
                
                # 合并大金额和聚合后的小金额库存地点
                final_df = pd.concat([large_df, small_df_agg], axis=0).reset_index(drop=True)
            else:
                final_df = large_df.copy()
            
            # 存储处理后的表格
            self.processed_tables[table_name] = final_df
            print(f"成功处理表格 '{table_name}', 处理后行数: {len(final_df)}")
            
            return True
            
        except Exception as e:
            print(f"处理表格 '{table_name}' 时出错: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


class ThresholdInputDialog(QDialog):
    """金额阈值输入对话框"""
    
    def __init__(self, parent=None, default_value=3000):
        super().__init__(parent)
        self.setWindowTitle("输入金额阈值")
        self.setGeometry(300, 300, 300, 100)
        
        layout = QFormLayout()
        self.threshold_input = QLineEdit(str(default_value))
        layout.addRow("金额阈值(万元):", self.threshold_input)
        
        self.ok_button = QPushButton("确定")
        self.ok_button.clicked.connect(self.accept)
        
        self.cancel_button = QPushButton("取消")
        self.cancel_button.clicked.connect(self.reject)
        
        button_layout = QHBoxLayout()
        button_layout.addWidget(self.ok_button)
        button_layout.addWidget(self.cancel_button)
        
        main_layout = QVBoxLayout()
        main_layout.addLayout(layout)
        main_layout.addLayout(button_layout)
        
        self.setLayout(main_layout)
    
    def get_threshold(self):
        """获取输入的阈值"""
        try:
            return float(self.threshold_input.text())
        except ValueError:
            return 3000.0  # 默认值

class ExcelProcessorApp(QMainWindow):
    """Excel处理应用的GUI界面"""
    
    def __init__(self):
        super().__init__()
        
        self.excel_processor = ExcelProcessor()
        self.threshold = 3000.0  # 默认阈值
        self.init_ui()
        
    def init_ui(self):
        """初始化用户界面"""
        self.setWindowTitle("Excel处理工具")
        self.setGeometry(100, 100, 1200, 800)
        
        # 主布局
        main_layout = QVBoxLayout()
        
        # 文件选择区域
        file_layout = QHBoxLayout()
        self.file_label = QLabel("未选择文件")
        self.file_button = QPushButton("选择Excel文件")
        self.file_button.clicked.connect(self.select_file)
        
        file_layout.addWidget(self.file_label)
        file_layout.addWidget(self.file_button)
        
        # 处理按钮区域
        button_layout = QHBoxLayout()
        self.process_button = QPushButton("读取表格")
        self.process_button.clicked.connect(self.process_excel)
        
        self.process_overdue_button = QPushButton("处理'逾期还款业务'表")
        self.process_overdue_button.clicked.connect(self.process_overdue_payment)
        self.process_overdue_button.setEnabled(False)
        
        self.process_delivery_button = QPushButton("处理'付款逾期未到货(1)'表")
        self.process_delivery_button.clicked.connect(self.process_overdue_delivery)
        self.process_delivery_button.setEnabled(False)
        
        self.process_inventory_button = QPushButton("处理'现货敞口90天及以上库存'表")
        self.process_inventory_button.clicked.connect(self.process_inventory)
        self.process_inventory_button.setEnabled(False)
        
        self.export_button = QPushButton("导出处理结果")
        self.export_button.clicked.connect(self.export_results)
        self.export_button.setEnabled(False)
        
        button_layout.addWidget(self.process_button)
        button_layout.addWidget(self.process_overdue_button)
        button_layout.addWidget(self.process_delivery_button)
        button_layout.addWidget(self.process_inventory_button)
        button_layout.addWidget(self.export_button)
        
        # 状态标签
        self.status_label = QLabel("准备就绪")
        
        # 选项卡控件，用于显示读取的表格
        self.tab_widget = QTabWidget()
        
        # 添加所有控件到主布局
        main_layout.addLayout(file_layout)
        main_layout.addLayout(button_layout)
        main_layout.addWidget(self.status_label)
        main_layout.addWidget(self.tab_widget)
        
        # 设置中央窗口
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)
        
    def select_file(self):
        """打开文件选择对话框"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择Excel文件", "", "Excel Files (*.xlsx *.xls)")
        
        if file_path:
            self.file_label.setText(file_path)
            self.excel_processor.set_file_path(file_path)
            self.status_label.setText(f"已选择文件: {file_path}")
    
    def process_excel(self):
        """处理Excel文件"""
        if not self.excel_processor.file_path:
            self.status_label.setText("请先选择一个Excel文件")
            return
            
        self.status_label.setText("正在读取Excel文件...")
        success = self.excel_processor.read_excel()
        
        if success:
            tables = self.excel_processor.get_tables()
            self.status_label.setText(f"成功读取Excel文件中的{len(tables)}个表格")
            self.display_tables()
            self.process_overdue_button.setEnabled(True)
            self.process_delivery_button.setEnabled(True)
            self.process_inventory_button.setEnabled(True)
        else:
            self.status_label.setText("读取Excel文件失败")
    
    def process_overdue_payment(self):
        """处理'一、逾期还款业务'表"""
        threshold = self.get_threshold()
        if threshold is None:
            return
            
        success = self.excel_processor.process_overdue_payment(threshold)
        
        if success:
            self.status_label.setText(f"成功处理'一、逾期还款业务'表 (阈值: {threshold}万)")
            self.display_processed_table("一、逾期还款业务")
            self.export_button.setEnabled(True)
        else:
            self.status_label.setText("处理'一、逾期还款业务'表失败")
    
    def process_overdue_delivery(self):
        """处理'二、付款逾期未到货(1)'表"""
        threshold = self.get_threshold()
        if threshold is None:
            return
            
        success = self.excel_processor.process_overdue_delivery(threshold)
        
        if success:
            self.status_label.setText(f"成功处理'二、付款逾期未到货(1)'表 (阈值: {threshold}万)")
            self.display_processed_table("二、付款逾期未到货(1)")
            self.export_button.setEnabled(True)
        else:
            self.status_label.setText("处理'二、付款逾期未到货(1)'表失败")
    
    def process_inventory(self):
        """处理'八、现货敞口90天及以上库存'表"""
        threshold = self.get_threshold()
        if threshold is None:
            return
            
        success = self.excel_processor.process_inventory(threshold)
        
        if success:
            self.status_label.setText(f"成功处理'八、现货敞口90天及以上库存'表 (阈值: {threshold}万)")
            self.display_processed_table("八、现货敞口90天及以上库存")
            self.export_button.setEnabled(True)
        else:
            self.status_label.setText("处理'八、现货敞口90天及以上库存'表失败")
    
    def export_results(self):
        """导出处理结果到Excel文件"""
        if not self.excel_processor.processed_tables:
            self.status_label.setText("没有可导出的处理结果")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "保存处理结果", "", "Excel Files (*.xlsx)")
            
        if not file_path:
            return
            
        if not file_path.endswith('.xlsx'):
            file_path += '.xlsx'
            
        try:
            # 创建一个ExcelWriter对象
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # 遍历所有处理过的表格并导出
                for table_name, table_data in self.excel_processor.processed_tables.items():
                    # 生成sheet名称（去掉"一、"、"二、"等前缀）
                    sheet_name = table_name.split("、")[1] if "、" in table_name else table_name
                    # 如果sheet名称太长，截取前31个字符（Excel的sheet名称长度限制）
                    sheet_name = sheet_name[:31]
                    # 导出到对应的sheet
                    table_data.to_excel(writer, sheet_name=sheet_name, index=False)
            
            self.status_label.setText(f"处理结果已导出到: {file_path}")
            
            # 询问是否打开导出的文件
            reply = QMessageBox.question(
                self, "导出成功", 
                f"处理结果已成功导出到:\n{file_path}\n\n是否立即打开该文件?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                # 使用默认程序打开Excel文件
                os.startfile(file_path)
                
        except Exception as e:
            self.status_label.setText(f"导出失败: {str(e)}")
    
    def display_tables(self):
        """在选项卡中显示读取的表格"""
        # 清除现有选项卡
        self.tab_widget.clear()
        
        # 获取所有表格
        tables = self.excel_processor.get_tables()
        
        for table_name, table_data in tables.items():
            # 创建表格视图
            table_view = QTableView()
            model = PandasModel(table_data)
            table_view.setModel(model)
            
            # 自动调整列宽以适应内容
            header = table_view.horizontalHeader()
            if header is not None:
                header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            
            # 将表格视图添加到新选项卡 - 截取表格名称中"、"后的内容，最多4个字符
            tab_name = table_name.split("、")[1][:4] if "、" in table_name else table_name[:4]
            self.tab_widget.addTab(table_view, tab_name)
    
    def display_processed_table(self, table_name: str):
        """在新选项卡中显示处理后的表格"""
        processed_table = self.excel_processor.get_processed_table(table_name)
        
        if processed_table is None:
            return
            
        # 创建表格视图
        table_view = QTableView()
        model = PandasModel(processed_table)
        table_view.setModel(model)
        
        # 自动调整列宽以适应内容
        header = table_view.horizontalHeader()
        if header is not None:
            header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        
        # 添加新选项卡，使用"处理后"作为标识
        tab_name = f"{table_name.split('、')[1][:4]}(处理后)" if "、" in table_name else f"{table_name[:4]}(处理后)"
        self.tab_widget.addTab(table_view, tab_name)
        
        # 切换到新选项卡
        self.tab_widget.setCurrentIndex(self.tab_widget.count() - 1)

    def get_threshold(self):
        """获取用户输入的金额阈值"""
        dialog = ThresholdInputDialog(self, self.threshold)
        result = dialog.exec()
        
        if result == QDialog.DialogCode.Accepted:
            self.threshold = dialog.get_threshold()
            return self.threshold
        else:
            return None


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ExcelProcessorApp()
    window.show()
    sys.exit(app.exec()) 