package com.linghit.domain;

// TODO sdklog字段
public class SdkLogEntity {
        // 反馈数据示例
        /**{
            "_index": "sdklog-2020-02-07",
            "_type": "_doc",
            "_id": "TnIsH3ABc4dl0FHNH5Gt",
            "_version": 1,
            "_score": null,
            "_source": {
                "event_id": "$EnterPage",
                "target_type": "cookie",
                "app_id": "X01",
                "@version": "1",
                "log_id": "056b1fa6-437b-4510-858f-b9d4cac36348",
                "log_version": "3.0",
                "log_type": "user_event",
                "original_id": "",
                "log_time": 1581070950,
                "user_id": "b8b904af-be33-4f85-8cfc-b3492480428a",
                "attr": {
                    "$channel": "app_az_1007_ronghe",
                    "$ip": "117.172.170.107",
                    "$country": "中国",
                    "$app_version": "",
                    "$equipment": "Netscape",
                    "$sys": "Linux armv8l",
                    "$sys_version": "Linux armv8l",
                    "$url": "https://rh.2m002bd.cn/?channel=app_az_1007_ronghe",
                    "$port": "13550",
                    "cookie": "b8b904af-be33-4f85-8cfc-b3492480428a",
                    "$equipment_id": "",
                    "$province": "四川",
                    "$useragent": "Mozilla/5.0 (Linux; Android 9; V1838T Build/PKQ1.190302.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/68.0.3440.91 Mobile Safari/537.36 linghit fslp cn/4.0.6 {p/200}{lang/0}{zxcs_method/100}",
                    "$screen_width": 360,
                    "$city": "宜宾",
                    "schannel": "",
                    "$language": "zh-CN",
                    "$scan_time": "",
                    "openid": "",
                    "$screen_size": "360 * 780",
                    "$screen_height": 780,
                    "$title": "灵机",
                    "$referrer": "",
                    "$sku_good_id": "",
                    "$phone": ""
                },
                "@timestamp": "2020-02-07T10:22:29.859Z",
                        "type": "sdklog",
                        "sdk": {
                            "sdk_id": "js",
                            "sdk_version": "3.0.3",
                            "sdk_method": "track"
                        }
                },
                "fields": {
                    "@timestamp": [
                    "2020-02-07T10:22:29.859Z"
                    ]
                },
                "highlight": {
                    "app_id": [
                    "@kibana-highlighted-field@X01@/kibana-highlighted-field@"
                    ]
                },
                "sort": [
                    1581070949859
                ]
            }
         **/

        public SdkLogEntity(){
        }

        private String address;
        private ContentBean content;
        private int status;
        private String message;

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public ContentBean getContent() {
            return content;
        }

        public void setContent(ContentBean content) {
            this.content = content;
        }

        public int getStatus() {
            return status;
        }

        public void setStatus(int status) {
            this.status = status;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public static class ContentBean {
            /*
             * address : 北京市                      #简要地址信息
             *
             * address_detail : city: "北京市",      #城市
             *                  city_code: 131,     #百度城市代码
             *                  district: "",       #区县
             *                  province: "北京市",  #省份
             *                  street: "",         #街道
             *                  street_number: ""   #门牌号
             *
             * point : x: "116.39564504",           #当前城市中心点经度
             *         y: "39.92998578"             #当前城市中心点纬度
             *
             * status : 0                           #结果状态返回码
             */

            private String address;                     // 简要地址信息
            private AddressDetailBean addressDetail;    // 结构化地址信息
            private PointBean point;                    // 当前城市中心点
            private String status;                      // 结果状态返回码

            public String getAddress() {
                return address;
            }

            public void setAddress(String address) {
                this.address = address;
            }

            public AddressDetailBean getAddressDetail() {
                return addressDetail;
            }

            public void setAddressDetail(AddressDetailBean addressDetail) {
                this.addressDetail = addressDetail;
            }

            public PointBean getPoint() {
                return point;
            }

            public void setPoint(PointBean point) {
                this.point = point;
            }

            public String getStatus() {
                return status;
            }

            public void setStatus(String status) {
                this.status = status;
            }

            public static class AddressDetailBean {
                /*
                 * city: "北京市",      #城市
                 * city_code: 131,     #百度城市代码
                 * district: "",       #区县
                 * province: "北京市",  #省份
                 * street: "",         #街道
                 * street_number: ""   #门牌号
                 */

                private String city;
                private String cityCode;
                private String district;
                private String province;
                private String street;
                private String street_number;

                public String getCity() {
                    return city;
                }

                public void setCity(String city) {
                    this.city = city;
                }

                public String getCityCode() {
                    return cityCode;
                }

                public void setCityCode(String cityCode) {
                    this.cityCode = cityCode;
                }

                public String getDistrict() {
                    return district;
                }

                public void setDistrict(String district) {
                    this.district = district;
                }

                public String getProvince() {
                    return province;
                }

                public void setProvince(String province) {
                    this.province = province;
                }

                public String getStreet() {
                    return street;
                }

                public void setStreet(String street) {
                    this.street = street;
                }

                public String getStreet_number() {
                    return street_number;
                }

                public void setStreet_number(String street_number) {
                    this.street_number = street_number;
                }
            }

            public static class PointBean {
                /*
                 * x: "116.39564504",   #当前城市中心点经度
                 * y: "39.92998578"     #当前城市中心点纬度
                 */

                private String x;
                private String y;

                public String getX() {
                    return x;
                }

                public void setX(String x) {
                    this.x = x;
                }

                public String getY() {
                    return y;
                }

                public void setY(String y) {
                    this.y = y;
                }
            }

        }

    }
