const data = {
}

export default {
  'POST /api/user/logout': (req: any, res: any) => {
    res.json({
      resultCode: 0,
      data: data,
    });
  },
};
