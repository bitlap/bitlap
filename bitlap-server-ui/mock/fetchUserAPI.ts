const data = {
  "id": "abcdroot",
  "name": "root",
  "type": "root",
  "avatar": "https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png"
}

export default {
  'POST /api/user/getUserById': (req: any, res: any) => {
    res.json({
      resultCode: 0,
      data: data,
    });
  },
};
