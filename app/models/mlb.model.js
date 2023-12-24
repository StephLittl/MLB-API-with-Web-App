module.exports = (sequelize, Sequelize) => {
    const Mlb = sequelize.define("mlb", {
      title: {
        type: Sequelize.STRING
      },
      description: {
        type: Sequelize.STRING
      },
      published: {
        type: Sequelize.BOOLEAN
      }
    });
  
    return Mlb;
  };