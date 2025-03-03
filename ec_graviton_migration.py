import boto3
import os
import re
import time
import pandas as pd
import json
from concurrent.futures import ThreadPoolExecutor, as_completed


class GravitonAdvisor:
    # AWS 区域映射
    REGION_LOCATION_MAP = {
        "us-east-1": "US East (N. Virginia)",
        "us-east-2": "US East (Ohio)",
        "us-west-1": "US West (N. California)",
        "us-west-2": "US West (Oregon)",
        "af-south-1": "Africa (Cape Town)",
        "ap-east-1": "Asia Pacific (Hong Kong)",
        "ap-south-1": "Asia Pacific (Mumbai)",
        "ap-northeast-1": "Asia Pacific (Tokyo)",
        "ap-northeast-2": "Asia Pacific (Seoul)",
        "ap-northeast-3": "Asia Pacific (Osaka)",
        "ap-southeast-1": "Asia Pacific (Singapore)",
        "ap-southeast-2": "Asia Pacific (Sydney)",
        "ap-southeast-3": "Asia Pacific (Jakarta)",
        "ca-central-1": "Canada (Central)",
        "eu-central-1": "EU (Frankfurt)",
        "eu-west-1": "EU (Ireland)",
        "eu-west-2": "EU (London)",
        "eu-west-3": "EU (Paris)",
        "eu-north-1": "EU (Stockholm)",
        "eu-south-1": "EU (Milan)",
        "me-south-1": "Middle East (Bahrain)",
        "sa-east-1": "South America (Sao Paulo)"
    }

    def __init__(self):
        # 从环境变量获取凭证
        self.session = boto3.Session(
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name="us-east-1"
        )
        self.pricing_client = self.session.client("pricing")
        self.price_cache = {}
        self.gpu_instance_families = ['p', 'g', 'vt', 'dl', 'trn', 'inf']
        self.graviton_pattern = re.compile(r"^[a-z]\d+g\.")

    def is_gpu_instance(self, instance_type):
        """检查是否为GPU实例"""
        if not instance_type:
            return False
        prefix = instance_type.split('.')[0]
        family = prefix[0]
        return family in self.gpu_instance_families

    def is_graviton_instance(self, instance_type):
        """检查是否已经是Graviton实例"""
        if not instance_type:
            return False
        return bool(self.graviton_pattern.match(instance_type))

    def fetch_prices_for_region(self, region):
        """获取特定区域的价格数据"""
        location = self.REGION_LOCATION_MAP.get(region)
        if not location:
            print(f"[WARNING] 未知区域: {region}")
            return {}

        filters = [
            {"Type": "TERM_MATCH", "Field": "location", "Value": location},
            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
        ]

        price_data = {}
        next_token = None
        retries = 3

        while retries > 0:
            try:
                kwargs = {
                    "ServiceCode": "AmazonEC2",
                    "Filters": filters,
                    "MaxResults": 100,
                }
                if next_token:
                    kwargs["NextToken"] = next_token

                response = self.pricing_client.get_products(**kwargs)

                for price_list_item in response.get("PriceList", []):
                    product_data = self._extract_price_from_product(price_list_item)
                    if product_data:
                        instance_type, price = product_data
                        price_data[instance_type] = price

                next_token = response.get("NextToken")
                if not next_token:
                    break

                # 避免API限制
                time.sleep(0.5)

            except Exception as e:
                print(f"[ERROR] 获取{region}价格失败: {str(e)}")
                retries -= 1
                time.sleep(2)
                if retries == 0:
                    print(f"[ERROR] 放弃获取{region}价格")
                    break

        return price_data

    def _extract_price_from_product(self, price_list_item):
        """从产品数据中提取实例类型和价格"""
        try:
            data = json.loads(price_list_item)
            attributes = data.get("product", {}).get("attributes", {})
            instance_type = attributes.get("instanceType")
            operating_system = attributes.get("operatingSystem")

            # 检查是否为SQL或其他特殊软件版本的Linux
            preinstalled_sw = attributes.get("preInstalledSw", "")
            if preinstalled_sw and preinstalled_sw != "NA":
                return None  # 排除预装软件的实例

            # 仅获取标准Linux价格
            if operating_system != "Linux":
                return None

            # 只提取按需价格
            terms = data.get("terms", {}).get("OnDemand", {})
            if not terms:
                return None

            # 获取第一个定价项
            price_dimensions = next(iter(terms.values())).get("priceDimensions", {})
            if not price_dimensions:
                return None

            # 获取第一个价格
            price_item = next(iter(price_dimensions.values()))
            price_str = price_item.get("pricePerUnit", {}).get("USD")
            if price_str:
                return instance_type, float(price_str)

            return None
        except Exception:
            return None

    def load_all_prices(self):
        """并行加载所有区域的价格"""
        print("[INFO] 正在加载AWS价格数据...")
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_region = {
                executor.submit(self.fetch_prices_for_region, region): region
                for region in self.REGION_LOCATION_MAP
            }

            for future in as_completed(future_to_region):
                region = future_to_region[future]
                try:
                    self.price_cache[region] = future.result()
                    print(f"[INFO] 已加载 {region} 的价格数据")
                except Exception as e:
                    print(f"[ERROR] 加载 {region} 价格失败: {str(e)}")

    def get_price(self, instance_type, region):
        """获取实例价格"""
        if not instance_type or not region:
            return None
        return self.price_cache.get(region, {}).get(instance_type)

    def match_graviton_instances(self, instance_type):
        """生成Graviton对应的实例类型"""
        if not instance_type:
            return None, None, None

        # 提取实例系列和大小
        match = re.match(r"^([a-z])(\d+)([a-z]*)\.(.+)$", instance_type)
        if not match:
            return None, None, None

        family, generation, suffix, size = match.groups()

        # GPU和Graviton实例不适用于迁移
        if self.is_gpu_instance(instance_type) or self.is_graviton_instance(instance_type):
            return None, None, None

        # 特殊处理t系列，只有t4g
        if family == 't':
            return f't4g.{size}', None, None

        # 对于常见的实例系列，生成对应的Graviton2、Graviton3和Graviton4版本
        if family in ['c', 'm', 'r']:
            g2 = f'{family}6g.{size}'
            g3 = f'{family}7g.{size}'
            g4 = f'{family}8g.{size}' if family != 'r' else None  # r8g可能尚未推出
            return g2, g3, g4

        # 特殊处理X系列
        if family == 'x':
            return f'x2g.{size}', None, None

        # 默认只返回Graviton2版本
        return f'{family}6g.{size}', None, None

    def analyze_instance(self, instance_name, instance_type, region, platform_details, instance_lifecycle):
        """分析实例迁移到Graviton的可能性"""
        # 检查是否为spot实例
        is_spot = instance_lifecycle == 'spot'

        # 检查是否为Windows系统
        is_windows = platform_details and "windows" in platform_details.lower()

        # 检查是否为GPU或Graviton实例
        is_gpu = self.is_gpu_instance(instance_type)
        is_graviton = self.is_graviton_instance(instance_type)

        # 获取价格
        original_price = self.get_price(instance_type, region)

        # 初始化结果
        result = {
            "InstanceName": instance_name,
            "InstanceType": instance_type,
            "Region": region,
            "x86_OD_Price": original_price,
            "Graviton_Status": "未知",
            "Graviton2": None,
            "Graviton2_OD_Price": None,
            "Savings_Graviton2%": None,
            "Graviton3": None,
            "Graviton3_OD_Price": None,
            "Savings_Graviton3%": None,
            "Graviton4": None,
            "Graviton4_OD_Price": None,
            "Savings_Graviton4%": None
        }

        # 判断迁移状态
        if is_windows:
            result["Graviton_Status"] = "OS不支持"
        elif is_gpu:
            result["Graviton_Status"] = "GPU实例不支持"
        elif is_graviton:
            result["Graviton_Status"] = "已是Graviton实例"
        elif is_spot:
            result["Graviton_Status"] = "Spot实例不参与转换"
        else:
            # 获取对应的Graviton实例类型
            g2, g3, g4 = self.match_graviton_instances(instance_type)

            # 检查区域中是否有对应的Graviton实例
            g2_price = self.get_price(g2, region) if g2 else None
            g3_price = self.get_price(g3, region) if g3 else None
            g4_price = self.get_price(g4, region) if g4 else None

            # 填充结果
            result["Graviton2"] = g2
            result["Graviton2_OD_Price"] = g2_price
            result["Graviton3"] = g3
            result["Graviton3_OD_Price"] = g3_price
            result["Graviton4"] = g4
            result["Graviton4_OD_Price"] = g4_price

            # 计算节省百分比
            if original_price and g2_price:
                result["Savings_Graviton2%"] = round((1 - g2_price / original_price) * 100, 2)
            if original_price and g3_price:
                result["Savings_Graviton3%"] = round((1 - g3_price / original_price) * 100, 2)
            if original_price and g4_price:
                result["Savings_Graviton4%"] = round((1 - g4_price / original_price) * 100, 2)

            # 判断最终状态
            if g2_price or g3_price or g4_price:
                result["Graviton_Status"] = "可以转换"
            else:
                result["Graviton_Status"] = "区域内无对应Graviton实例"

        return result

    def process_csv(self, input_file, output_file):
        """处理CSV文件并生成报告"""
        print(f"[INFO] 正在读取输入文件: {input_file}")
        try:
            # 尝试不同编码读取CSV文件
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
            df = None

            for encoding in encodings:
                try:
                    df = pd.read_csv(input_file, encoding=encoding)
                    print(f"[INFO] 成功使用 {encoding} 编码读取文件")
                    break
                except UnicodeDecodeError:
                    continue

            if df is None:
                raise Exception("无法读取CSV文件，请检查文件格式和编码")

            # 规范化列名
            column_mapping = {
                'instanceName': 'InstanceName',
                'instanceType': 'InstanceType',
                'az': 'AZ',
                'platformDetails': 'PlatformDetails',
                'instanceLifecycle': 'InstanceLifecycle'
            }
            df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})

            # 提取区域信息
            if 'Region' not in df.columns and 'AZ' in df.columns:
                df['Region'] = df['AZ'].apply(lambda x: x[:-1] if isinstance(x, str) and len(x) > 2 else x)

            # 准备结果列表
            results = []
            print("[INFO] 开始分析实例...")

            # 用于汇总的字典
            summary = {}

            # 处理每一行
            for _, row in df.iterrows():
                instance_name = row.get('InstanceName', '')
                instance_type = row.get('InstanceType', '')
                region = row.get('Region', '')
                platform_details = row.get('PlatformDetails', '')
                instance_lifecycle = row.get('InstanceLifecycle', '')

                # 分析实例
                result = self.analyze_instance(instance_name, instance_type, region, platform_details,
                                               instance_lifecycle)
                results.append(result)
                if result["Graviton_Status"] == "可以转换":
                    key = (instance_type, region)
                    if key not in summary:
                        summary[key] = {
                            "InstanceType": instance_type,
                            "Region": region,
                            "Count": 0,
                            "OD_Price": result["x86_OD_Price"],
                            "Graviton2": result["Graviton2"],
                            "Graviton2_OD_Price": result["Graviton2_OD_Price"],
                            "Savings_Graviton2%": result["Savings_Graviton2%"],
                            "Graviton3": result["Graviton3"],
                            "Graviton3_OD_Price": result["Graviton3_OD_Price"],
                            "Savings_Graviton3%": result["Savings_Graviton3%"],
                            "Graviton4": result["Graviton4"],
                            "Graviton4_OD_Price": result["Graviton4_OD_Price"],
                            "Savings_Graviton4%": result["Savings_Graviton4%"]
                        }
                    summary[key]["Count"] += 1

            # 创建结果DataFrame
            result_df = pd.DataFrame(results)

            # 创建汇总DataFrame
            summary_df = pd.DataFrame(list(summary.values()))

            # 保存结果
            with pd.ExcelWriter(output_file) as writer:
                result_df.to_excel(writer, sheet_name='Instances', index=False)
                if not summary_df.empty:
                    summary_df.to_excel(writer, sheet_name='Group', index=False)
            print(f"[INFO] 分析完成，结果已保存到: {output_file}")

        except Exception as e:
            print(f"[ERROR] 处理文件时出错: {str(e)}")
            import traceback
            traceback.print_exc()

def main():
    """主函数"""
    # 获取文件路径
    input_file = input("请输入EC2实例信息CSV文件路径: ")
    output_file = input("请输入输出Excel文件路径: ")

    # 初始化顾问
    advisor = GravitonAdvisor()

    # 加载价格数据
    advisor.load_all_prices()

    # 处理数据
    advisor.process_csv(input_file, output_file)

if __name__ == "__main__":
    main()