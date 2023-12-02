const data = {
  "username": "root",
  "avatar": "https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png"
}

export default {
  'GET /api/user/getUserByName': (req: any, res: any) => {
    res.json({
      resultCode: 0,
      params: data,
    });
  },
};
