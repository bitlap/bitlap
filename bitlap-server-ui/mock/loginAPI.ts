const data = {
  "username": "root",
  "avatar": "https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png"
}

export default {
  'POST /api/user/login': (req: any, res: any) => {
    res.json({
      code: 0,
      data: data,
    });
  },
};
